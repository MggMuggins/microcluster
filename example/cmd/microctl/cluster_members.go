package main

import (
	"io"
	"os"
	"sort"

	"github.com/canonical/lxd/shared"
	cli "github.com/canonical/lxd/shared/cmd"
	"github.com/canonical/lxd/shared/termios"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v2"

	"github.com/canonical/microcluster/client"
	"github.com/canonical/microcluster/microcluster"
)

type cmdClusterMembers struct {
	common *CmdControl
}

func (c *cmdClusterMembers) command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "manage cluster members.",
		RunE:  c.run,
	}

	var cmdRemove = cmdClusterMemberRemove{common: c.common}
	cmd.AddCommand(cmdRemove.command())

	var cmdList = cmdClusterMembersList{common: c.common}
	cmd.AddCommand(cmdList.command())

	return cmd
}

func (c *cmdClusterMembers) run(cmd *cobra.Command, args []string) error {
	return cmd.Help()
}

type cmdClusterMembersList struct {
	common *CmdControl
}

func (c *cmdClusterMembersList) command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <address>",
		Short: "List cluster members locally, or remotely if an address is specified.",
		RunE:  c.run,
	}

	return cmd
}

func (c *cmdClusterMembersList) run(cmd *cobra.Command, args []string) error {
	if len(args) > 1 {
		return cmd.Help()
	}

	// Get all state information for MicroCluster.
	m, err := microcluster.App(microcluster.Args{StateDir: c.common.FlagStateDir, Verbose: c.common.FlagLogVerbose, Debug: c.common.FlagLogDebug})
	if err != nil {
		return err
	}

	var client *client.Client

	// Get a local client connected to the unix socket if no address is specified.
	if len(args) == 1 {
		client, err = m.RemoteClient(args[0])
		if err != nil {
			return err
		}
	} else {
		client, err = m.LocalClient()
		if err != nil {
			return err
		}
	}

	clusterMembers, err := client.GetClusterMembers(cmd.Context())
	if err != nil {
		return err
	}

	data := make([][]string, len(clusterMembers))
	for i, clusterMember := range clusterMembers {
		data[i] = []string{clusterMember.Name, clusterMember.Address.String(), clusterMember.Role, clusterMember.Certificate.String(), string(clusterMember.Status)}
	}

	header := []string{"NAME", "ADDRESS", "ROLE", "CERTIFICATE", "STATUS"}
	sort.Sort(cli.SortColumnsNaturally(data))

	return cli.RenderTable(cli.TableFormatTable, header, data, clusterMembers)
}

type cmdClusterMemberRemove struct {
	common *CmdControl

	flagForce bool
}

func (c *cmdClusterMemberRemove) command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove <name>",
		Short: "Remove the cluster member with the given name.",
		RunE:  c.run,
	}

	cmd.Flags().BoolVarP(&c.flagForce, "force", "f", false, "Forcibly remove the cluster member")

	return cmd
}

func (c *cmdClusterMemberRemove) run(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return cmd.Help()
	}

	m, err := microcluster.App(microcluster.Args{StateDir: c.common.FlagStateDir, Verbose: c.common.FlagLogVerbose, Debug: c.common.FlagLogDebug})
	if err != nil {
		return err
	}

	client, err := m.LocalClient()
	if err != nil {
		return err
	}

	err = client.DeleteClusterMember(cmd.Context(), args[0], c.flagForce)
	if err != nil {
		return err
	}

	return nil
}

type cmdRecover struct {
	common *CmdControl

	//flagForce bool
}

func (c *cmdRecover) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recover",
		Short: "Recover the cluster from this node if quorum is lost",
		RunE:  c.Run,
	}

	//cmd.Flags().BoolVarP(&c.flagForce, "force", "f", false, "Forcibly remove the cluster member")

	return cmd
}

func (c *cmdRecover) Run(cmd *cobra.Command, args []string) error {
	/*if len(args) != 1 {
		return cmd.Help()
	}*/

	m, err := microcluster.App(microcluster.Args{StateDir: c.common.FlagStateDir, Verbose: c.common.FlagLogVerbose, Debug: c.common.FlagLogDebug})
	if err != nil {
		return err
	}

	members, err := m.GetClusterMembers()
	if err != nil {
		return err
	}

	membersYaml, err := yaml.Marshal(members)
	if err != nil {
		return err
	}

	var content []byte
	if !termios.IsTerminal(unix.Stdin) {
		content, err = io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
	} else {
		//FIXME: Editor comment

		content, err = shared.TextEditor("", membersYaml)
		if err != nil {
			return err
		}
	}

	newMembers := []microcluster.Member{}
	err = yaml.Unmarshal(content, &newMembers)

	err = m.RecoverFromQuorumLoss(newMembers)
	if err != nil {
		return err
	}

	return nil
}
