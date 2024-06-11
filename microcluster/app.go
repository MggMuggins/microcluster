package microcluster

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite"
	dqliteClient "github.com/canonical/go-dqlite/client"
	"github.com/canonical/lxd/lxd/db/schema"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	"golang.org/x/sys/unix"

	"github.com/canonical/microcluster/client"
	"github.com/canonical/microcluster/cluster"
	"github.com/canonical/microcluster/config"
	"github.com/canonical/microcluster/internal/daemon"
	internalClient "github.com/canonical/microcluster/internal/rest/client"
	internalTypes "github.com/canonical/microcluster/internal/rest/types"
	"github.com/canonical/microcluster/internal/sys"
	"github.com/canonical/microcluster/internal/trust"
	"github.com/canonical/microcluster/rest"
	"github.com/canonical/microcluster/rest/types"
)

// MicroCluster contains some basic filesystem information for interacting with the MicroCluster daemon.
type MicroCluster struct {
	FileSystem *sys.OS

	args Args
}

// Args contains options for configuring MicroCluster.
type Args struct {
	Verbose     bool
	Debug       bool
	StateDir    string
	SocketGroup string

	ListenPort string
	Client     *client.Client
	Proxy      func(*http.Request) (*url.URL, error)

	ExtensionServers []rest.Server
}

// App returns an instance of MicroCluster with a newly initialized filesystem if one does not exist.
func App(args Args) (*MicroCluster, error) {
	if args.StateDir == "" {
		return nil, fmt.Errorf("Missing state directory")
	}
	stateDir, err := filepath.Abs(args.StateDir)
	if err != nil {
		return nil, fmt.Errorf("Missing absolute state directory: %w", err)
	}
	os, err := sys.DefaultOS(stateDir, args.SocketGroup, true)
	if err != nil {
		return nil, err
	}

	m := &MicroCluster{
		FileSystem: os,
		args:       args,
	}

	m.maybeUnpackRecoveryTarball()

	return m, nil
}

// Start starts up a brand new MicroCluster daemon. Only the local control socket will be available at this stage, no
// database exists yet. Any api or schema extensions can be applied here.
// - `extensionsSchema` is a list of schema updates in the order that they should be applied.
// - `extensionsAPI` is a list of endpoints to be served over `/1.0`.
// - `hooks` are a set of functions that trigger at certain points during cluster communication.
func (m *MicroCluster) Start(ctx context.Context, extensionsAPI []rest.Endpoint, extensionsSchema []schema.Update, apiExtensions []string, hooks *config.Hooks) error {
	// Initialize the logger.
	err := logger.InitLogger(m.FileSystem.LogFile, "", m.args.Verbose, m.args.Debug, nil)
	if err != nil {
		return err
	}

	// Start up a daemon with a basic control socket.
	defer logger.Info("Daemon stopped")
	d := daemon.NewDaemon(cluster.GetCallerProject())

	chIgnore := make(chan os.Signal, 1)
	signal.Notify(chIgnore, unix.SIGHUP)

	ctx, cancel := signal.NotifyContext(ctx, unix.SIGPWR, unix.SIGTERM, unix.SIGINT, unix.SIGQUIT)
	defer cancel()

	err = d.Run(ctx, m.args.ListenPort, m.FileSystem.StateDir, m.FileSystem.SocketGroup, extensionsAPI, extensionsSchema, apiExtensions, m.args.ExtensionServers, hooks)
	if err != nil {
		return fmt.Errorf("Daemon stopped with error: %w", err)
	}

	return nil
}

// Status returns basic status information about the cluster.
func (m *MicroCluster) Status(ctx context.Context) (*internalTypes.Server, error) {
	c, err := m.LocalClient()
	if err != nil {
		return nil, err
	}

	server := internalTypes.Server{}
	err = c.QueryStruct(ctx, "GET", internalClient.PublicEndpoint, nil, nil, &server)
	if err != nil {
		return nil, fmt.Errorf("Failed to get cluster status: %w", err)
	}

	return &server, nil
}

// Ready waits for the daemon to report it has finished initial setup and is ready to be bootstrapped or join an
// existing cluster.
func (m *MicroCluster) Ready(ctx context.Context) error {
	finger := make(chan error, 1)
	var errLast error
	go func() {
		for i := 0; ; i++ {
			// Start logging only after the 10'th attempt (about 5
			// seconds). Then after the 30'th attempt (about 15
			// seconds), log only only one attempt every 10
			// attempts (about 5 seconds), to avoid being too
			// verbose.
			doLog := false
			if i > 10 {
				doLog = i < 30 || ((i % 10) == 0)
			}

			if doLog {
				logger.Debugf("Connecting to MicroCluster daemon (attempt %d)", i)
			}

			c, err := m.LocalClient()
			if err != nil {
				errLast = err
				if doLog {
					logger.Debugf("Failed connecting to MicroCluster daemon (attempt %d): %v", i, err)
				}

				time.Sleep(500 * time.Millisecond)
				continue
			}

			if doLog {
				logger.Debugf("Checking if MicroCluster daemon is ready (attempt %d)", i)
			}

			err = c.CheckReady(ctx)
			if err != nil {
				errLast = err
				if doLog {
					logger.Debugf("Failed to check if MicroCluster daemon is ready (attempt %d): %v", i, err)
				}

				time.Sleep(500 * time.Millisecond)
				continue
			}

			finger <- nil
			return
		}
	}()

	select {
	case <-finger:
	case <-ctx.Done():
		return fmt.Errorf("MicroCluster still not running after context deadline exceeded: %w", errLast)
	}

	return nil
}

// NewCluster bootstrapps a brand new cluster with this daemon as its only member.
func (m *MicroCluster) NewCluster(ctx context.Context, name string, address string, config map[string]string) error {
	c, err := m.LocalClient()
	if err != nil {
		return err
	}

	addr, err := types.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("Received invalid address %q: %w", address, err)
	}

	return c.ControlDaemon(ctx, internalTypes.Control{Bootstrap: true, Address: addr, Name: name, InitConfig: config})
}

// JoinCluster joins an existing cluster with a join token supplied by an existing cluster member.
func (m *MicroCluster) JoinCluster(ctx context.Context, name string, address string, token string, initConfig map[string]string) error {
	c, err := m.LocalClient()
	if err != nil {
		return err
	}

	addr, err := types.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("Received invalid address %q: %w", address, err)
	}

	return c.ControlDaemon(ctx, internalTypes.Control{JoinToken: token, Address: addr, Name: name, InitConfig: initConfig})
}

func dumpYamlNodeStore(path string) ([]dqliteClient.NodeInfo, error) {
	store, err := dqliteClient.NewYamlNodeStore(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read %q: %w", path, err)
	}

	nodeInfo, err := store.Get(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Failed to read from node store: %w", err)
	}

	return nodeInfo, nil
}

type Member struct {
	// dqlite.NodeInfo fields
	DqliteID uint64 `json:"id" yaml:"id"`
	Address  string `json:"address" yaml:"address"`
	Role     string `json:"role" yaml:"role"`

	Name string `json:"name" yaml:"name"`
}

func newMember(name string, info dqlite.NodeInfo) Member {
	return Member{
		DqliteID: info.ID,
		Address:  info.Address,
		Role:     info.Role.String(),
		Name:     name,
	}
}

func (m Member) toNodeInfo() (*dqlite.NodeInfo, error) {
	var role dqliteClient.NodeRole
	switch m.Role {
	case "voter":
		role = dqliteClient.Voter
	case "stand-by":
		role = dqliteClient.StandBy
	case "spare":
		role = dqliteClient.Spare
	default:
		return nil, fmt.Errorf("invalid dqlite role %q", m.Role)
	}

	return &dqlite.NodeInfo{
		ID:      m.DqliteID,
		Role:    role,
		Address: m.Address,
	}, nil
}

func (m *MicroCluster) GetClusterMembers() ([]Member, error) {
	storePath := path.Join(m.FileSystem.DatabaseDir, "cluster.yaml")
	nodeInfo, err := dumpYamlNodeStore(storePath)
	if err != nil {
		return nil, err
	}

	var members []Member
	for _, info := range nodeInfo {
		members = append(members, newMember("TBD", info))
	}

	return members, nil
}

// RecoverFromQuorumLoss can be used to recover database access when a quorum of
// members is lost and cannot be recovered (e.g. hardware failure or irreversible
// network changes).
// This function requires that:
//   - All cluster members' databases are not running
//   - The current member has the most up-to-date raft log (usually the member
//     which was most recently the leader)
//
// RecoverFromQuorumLoss will take a database backup before attempting the
// recovery operation.
//
// RecoverFromQuorumLoss should be invoked _exactly once_ for the entire cluster.
// This function creates an xz-compressed tarball
// path.Join(m.FileSystem.StateDir, "recovery_db.tar.xz"). This tarball should
// be manually copied by the user to the state dir of all other cluster members.
//
// On start, Microcluster will automatically check for & load the recovery
// tarball. A database backup will be taken before the load.
//
// RecoverFromQuorumLoss also updates the trust store with the new IP addresses
// provided in []Member.
func (m *MicroCluster) RecoverFromQuorumLoss(members []Member) error {
	// Double check to make sure the cluster configuration has actually changed
	oldMembers, err := m.GetClusterMembers()
	if err != nil {
		return err
	}

	countNewMembers := 0
	for _, newMember := range members {
		for _, oldMember := range oldMembers {
			if newMember.DqliteID == oldMember.DqliteID && newMember.Name == oldMember.Name {
				countNewMembers += 1
				break
			}
		}
	}

	if countNewMembers != len(oldMembers) {
		return fmt.Errorf("cluster members cannot be added or removed during recovery")
	}

	// Ensure we weren't passed any invalid addresses
	for _, member := range members {
		_, err = netip.ParseAddrPort(member.Address)
		if err != nil {
			return fmt.Errorf("Invalid address %q: %w", member.Address, err)
		}
	}

	// Set up our new cluster configuration
	nodeInfo := make([]dqlite.NodeInfo, 0, len(members))
	for _, member := range members {
		info, err := member.toNodeInfo()
		if err != nil {
			return err
		}
		nodeInfo = append(nodeInfo, *info)
	}

	// Check each cluster member's /1.0 endpoint to ensure that they are unreachable
	// This is a sanity check to ensure that we're not reconfiguring a cluster
	// that still has quorum
	// It may also be possible to check the raft term of each surviving member
	// and redirect the user to call RecoverFromQuorumLoss on that member instead.
	//TODO

	// Ensure that the daemon is not running
	socketPath := m.FileSystem.ControlSocketPath()
	_, err = os.Stat(socketPath)
	if err == nil {
		return fmt.Errorf("daemon is running (socket path exists: %q)", socketPath)
	}

	//FIXME: Take a DB backup

	fmt.Printf("ReconfigureMembership(%s, %v)\n", m.FileSystem.DatabaseDir, nodeInfo)

	err = dqlite.ReconfigureMembershipExt(m.FileSystem.DatabaseDir, nodeInfo)
	if err != nil {
		return fmt.Errorf("dqlite recovery: %w", err)
	}

	// Tar up the m.FileSystem.DatabaseDir and write to `dbExportPath`
	m.createRecoveryTarball()

	// Now that the DB has regained quorum, we can modify the entries in the
	// internal_cluster_members table to indicate that they are down
	//TODO - This can't be done here

	updateTrustStore(m.FileSystem.TrustDir, members)

	return nil
}

func (m *MicroCluster) createRecoveryTarball() error {
	dbFS := os.DirFS(m.FileSystem.DatabaseDir)
	dbFiles, err := fs.Glob(dbFS, "*")
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	tarballPath := path.Join(m.FileSystem.StateDir, "recovery_db.tar.gz")

	// info.yaml is used by go-dqlite to keep track of the current cluster member's
	// ID and address. We shouldn't replicate the recovery member's info.yaml
	// to all other members, so exclude it from the tarball:
	for indx, filename := range dbFiles {
		if filename == "info.yaml" {
			newlen := len(dbFiles) - 1
			dbFiles[indx] = dbFiles[newlen]
			dbFiles = dbFiles[:newlen]
			break
		}
	}

	return createTarball(tarballPath, m.FileSystem.DatabaseDir, dbFiles)
}

// create tarball at tarballPath with files path.Join(dir, file)
// Note: does not handle subdirectories
func createTarball(tarballPath string, dir string, files []string) error {
	tarball, err := os.Create(tarballPath)
	if err != nil {
		return fmt.Errorf("create recovery tarball %q: %w", tarballPath, err)
	}

	gzWriter := gzip.NewWriter(tarball)
	tarWriter := tar.NewWriter(gzWriter)

	for _, filename := range files {
		filepath := path.Join(dir, filename)

		file, err := os.Open(filepath)
		if err != nil {
			return fmt.Errorf("open %q: %w", filepath, err)
		}

		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("stat %q: %w", filepath, err)
		}

		// Note: header.Name is set to the basename of stat. If dqlite starts
		// using subdirs in the DB dir, this will need modification
		header, err := tar.FileInfoHeader(stat, filename)
		if err != nil {
			return fmt.Errorf("create tar header for %q: %w", filepath, err)
		}

		err = tarWriter.WriteHeader(header)
		if err != nil {
			return fmt.Errorf("write %q: %w", tarballPath, err)
		}

		_, err = io.Copy(tarWriter, file)
		if err != nil {
			return fmt.Errorf("write %q: %w", tarballPath, err)
		}

		err = file.Close()
		if err != nil {
			return fmt.Errorf("close %q: %w", filepath, err)
		}
	}

	err = tarWriter.Close()
	if err != nil {
		return fmt.Errorf("close recovery tarball %q: %w", tarballPath, err)
	}

	err = gzWriter.Close()
	if err != nil {
		return fmt.Errorf("close recovery tarball %q: %w", tarballPath, err)
	}

	err = tarball.Close()
	if err != nil {
		return fmt.Errorf("close recovery tarball %q: %w", tarballPath, err)
	}

	return nil
}

// Update the trust store with the new member addresses
func updateTrustStore(dir string, members []Member) error {
	fsWatcher, err := sys.NewWatcher(context.Background(), dir)
	if err != nil {
		return err
	}

	trustStore, err := trust.Init(fsWatcher, nil, dir)
	if err != nil {
		return err
	}

	remotes := trustStore.Remotes()
	remotesByName := remotes.RemotesByName()

	trustMembers := make([]internalTypes.ClusterMember, len(members))
	for _, member := range members {
		cert := remotesByName[member.Name].Certificate
		addr, err := netip.ParseAddrPort(member.Address)
		if err != nil {
			return fmt.Errorf("Invalid address %q: %w", member.Address, err)
		}

		trustMembers = append(trustMembers, internalTypes.ClusterMember{
			ClusterMemberLocal: internalTypes.ClusterMemberLocal{
				Name:        member.Name,
				Address:     types.AddrPort{AddrPort: addr},
				Certificate: cert,
			},
		})
	}

	return remotes.Replace(dir, trustMembers...)
}

// Check for the presence of a recovery tarball in stateDir. If it exists,
// unpack it into a temporary directory, ensure that it is a valid microcluster
// recovery tarball, and replace the existing databaseDir
func (m *MicroCluster) maybeUnpackRecoveryTarball() error {
	// Sanity checks:
	// - /metadata exists
	// - /cluster.yaml exists
	// - ?

	tarballPath := path.Join(m.FileSystem.StateDir, "recovery_db.tar.gz")
	unpackDir := path.Join(m.FileSystem.StateDir, "recovery_db")

	// Determine if the recovery tarball exists
	if _, err := os.Stat(tarballPath); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	err := unpackTarball(tarballPath, unpackDir)
	if err != nil {
		return err
	}

	//TODO Is this a reasonable sanity check?
	metadataPath := path.Join(unpackDir, "metadata1")
	if _, err := os.Stat(metadataPath); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("missing %q in recovery tarball", metadataPath)
	}

	// check for a valid cluster.yaml
	clusterYamlPath := path.Join(unpackDir, "cluster.yaml")
	_, err = dumpYamlNodeStore(clusterYamlPath)
	if err != nil {
		return err
	}

	// use the local info.yaml so that the dqlite ID is preserved on each
	// cluster member
	localInfoYamlPath := path.Join(m.FileSystem.DatabaseDir, "info.yaml")
	recoveryInfoYamlPath := path.Join(unpackDir, "info.yaml")

	infoYaml, err := os.ReadFile(localInfoYamlPath)
	if err != nil {
		return fmt.Errorf("read %q: %w", localInfoYamlPath, err)
	}

	err = os.WriteFile(recoveryInfoYamlPath, infoYaml, 0o644)
	if err != nil {
		return fmt.Errorf("write %q: %w", recoveryInfoYamlPath, err)
	}

	// Now that we're as sure as we can be that the recovery DB is valid, we can
	// replace the existing DB
	err = os.Rename(unpackDir, m.FileSystem.DatabaseDir)
	if err != nil {
		return fmt.Errorf("move %q to %q: %w", unpackDir, m.FileSystem.DatabaseDir, err)
	}

	// Prevent the database being "restored" after subsequent restarts
	err = os.Remove(tarballPath)
	if err != nil {
		return fmt.Errorf("remove %q: %w", tarballPath, err)
	}

	//TODO Update the trust store with new member addrs

	return nil
}

// Note: Does not handle subdirectories
func unpackTarball(tarballPath string, destRoot string) error {
	tarball, err := os.Open(tarballPath)
	if err != nil {
		return fmt.Errorf("open %q: %w", tarballPath, err)
	}

	gzReader, err := gzip.NewReader(tarball)
	if err != nil {
		return fmt.Errorf("decompress %q: %w", tarballPath, err)
	}

	tarReader := tar.NewReader(gzReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("unarchive %q: %w", tarballPath, err)
		}

		filepath := path.Join(destRoot, header.Name)
		file, err := os.Create(filepath)
		if err != nil {
			return fmt.Errorf("open %q: %w", filepath, err)
		}

		countWritten, err := io.Copy(file, tarReader)
		if countWritten != header.Size {
			return fmt.Errorf("mismatched written (%d) and size (%d) for entry %q in %q", countWritten, header.Size, header.Name, tarballPath)
		} else if err != nil {
			return fmt.Errorf("write %q: %w", filepath, err)
		}
	}

	return nil
}

// NewJoinToken creates and records a new join token containing all the necessary credentials for joining a cluster.
// Join tokens are tied to the server certificate of the joining node, and will be deleted once the node has joined the
// cluster.
func (m *MicroCluster) NewJoinToken(ctx context.Context, name string) (string, error) {
	c, err := m.LocalClient()
	if err != nil {
		return "", err
	}

	secret, err := c.RequestToken(ctx, name)
	if err != nil {
		return "", err
	}

	return secret, nil
}

// ListJoinTokens lists all the join tokens currently available for use.
func (m *MicroCluster) ListJoinTokens(ctx context.Context) ([]internalTypes.TokenRecord, error) {
	c, err := m.LocalClient()
	if err != nil {
		return nil, err
	}

	records, err := c.GetTokenRecords(ctx)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// RevokeJoinToken revokes the token record stored under the given name.
func (m *MicroCluster) RevokeJoinToken(ctx context.Context, name string) error {
	c, err := m.LocalClient()
	if err != nil {
		return err
	}

	err = c.DeleteTokenRecord(ctx, name)
	if err != nil {
		return err
	}

	return nil
}

// LocalClient returns a client connected to the local control socket.
func (m *MicroCluster) LocalClient() (*client.Client, error) {
	c := m.args.Client
	if c == nil {
		internalClient, err := internalClient.New(m.FileSystem.ControlSocket(), nil, nil, false)
		if err != nil {
			return nil, err
		}

		c = &client.Client{Client: *internalClient}
	}

	if m.args.Proxy != nil {
		tx, ok := c.Client.Client.Transport.(*http.Transport)
		if !ok {
			return nil, fmt.Errorf("Invalid underlying client transport, expected %T, got %T", &http.Transport{}, c.Client.Client.Transport)
		}

		tx.Proxy = m.args.Proxy
		c.Client.Client.Transport = tx
	}

	return c, nil
}

// RemoteClient gets a client for the specified cluster member URL.
// The filesystem will be parsed for the cluster and server certificates.
func (m *MicroCluster) RemoteClient(address string) (*client.Client, error) {
	c := m.args.Client
	if c == nil {
		serverCert, err := m.FileSystem.ServerCert()
		if err != nil {
			return nil, err
		}

		var publicKey *x509.Certificate
		clusterCert, err := m.FileSystem.ClusterCert()
		if err == nil {
			publicKey, err = clusterCert.PublicKeyX509()
			if err != nil {
				return nil, err
			}
		}

		url := api.NewURL().Scheme("https").Host(address)
		internalClient, err := internalClient.New(*url, serverCert, publicKey, false)
		if err != nil {
			return nil, err
		}

		c = &client.Client{Client: *internalClient}
	}

	if m.args.Proxy != nil {
		tx, ok := c.Client.Client.Transport.(*http.Transport)
		if !ok {
			return nil, fmt.Errorf("Invalid underlying client transport, expected %T, got %T", &http.Transport{}, c.Client.Client.Transport)
		}

		tx.Proxy = m.args.Proxy
		c.Client.Client.Transport = tx
	}

	return c, nil
}

// SQL performs either a GET or POST on /internal/sql with a given query. This is a useful helper for using direct SQL.
func (m *MicroCluster) SQL(ctx context.Context, query string) (string, *internalTypes.SQLBatch, error) {
	if query == "-" {
		// Read from stdin
		bytes, err := io.ReadAll(os.Stdin)
		if err != nil {
			return "", nil, fmt.Errorf("Failed to read from stdin: %w", err)
		}

		query = string(bytes)
	}

	c, err := m.LocalClient()
	if err != nil {
		return "", nil, err
	}

	if query == ".dump" || query == ".schema" {
		dump, err := c.GetSQL(ctx, query == ".schema")
		if err != nil {
			return "", nil, fmt.Errorf("failed to parse dump response: %w", err)
		}

		return fmt.Sprintf(dump.Text), nil, nil
	}

	data := internalTypes.SQLQuery{
		Query: query,
	}

	batch, err := c.PostSQL(ctx, data)

	return "", batch, err
}
