package recover

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/netip"
	"os"
	"path"

	dqlite "github.com/canonical/go-dqlite/client"
	"github.com/canonical/lxd/shared/logger"

	"github.com/canonical/microcluster/cluster"
	internalTypes "github.com/canonical/microcluster/internal/rest/types"
	"github.com/canonical/microcluster/internal/sys"
	"github.com/canonical/microcluster/internal/trust"
	"github.com/canonical/microcluster/rest/types"
)

func GetLocalClusterMembers(filesystem *sys.OS) ([]cluster.Member, error) {
	storePath := path.Join(filesystem.DatabaseDir, "cluster.yaml")
	nodeInfo, err := dumpYamlNodeStore(storePath)
	if err != nil {
		return nil, err
	}

	remotes, err := ReadTrustStore(filesystem.TrustDir)
	if err != nil {
		return nil, err
	}

	remotesByName := remotes.RemotesByName()

	var members []cluster.Member
	for _, remote := range remotesByName {
		for _, info := range nodeInfo {
			if remote.Address.String() == info.Address {
				members = append(members, cluster.Member{
					DqliteID: info.ID,
					Address:  info.Address,
					Role:     info.Role.String(),
					Name:     remote.Name,
				})
			}
		}
	}

	return members, nil
}

func dumpYamlNodeStore(path string) ([]dqlite.NodeInfo, error) {
	store, err := dqlite.NewYamlNodeStore(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read %q: %w", path, err)
	}

	nodeInfo, err := store.Get(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Failed to read from node store: %w", err)
	}

	return nodeInfo, nil
}

func ReadTrustStore(dir string) (*trust.Remotes, error) {
	remotes := &trust.Remotes{}
	err := remotes.Load(dir)

	return remotes, err
}

// Update the trust store with the new member addresses
func UpdateTrustStore(dir string, members []cluster.Member) error {
	remotes, err := ReadTrustStore(dir)
	if err != nil {
		return err
	}

	remotesByName := remotes.RemotesByName()

	trustMembers := make([]internalTypes.ClusterMember, 0, len(members))
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

	err = remotes.Replace(dir, trustMembers...)
	if err != nil {
		return fmt.Errorf("update trust store at %q: %w", dir, err)
	}

	return nil
}

func CreateRecoveryTarball(filesystem *sys.OS) error {
	dbFS := os.DirFS(filesystem.DatabaseDir)
	dbFiles, err := fs.Glob(dbFS, "*")
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	tarballPath := path.Join(filesystem.StateDir, "recovery_db.tar.gz")

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

	return createTarball(tarballPath, filesystem.DatabaseDir, dbFiles)
}

// Check for the presence of a recovery tarball in stateDir. If it exists,
// unpack it into a temporary directory, ensure that it is a valid microcluster
// recovery tarball, and replace the existing databaseDir
func MaybeUnpackRecoveryTarball(filesystem *sys.OS) error {
	tarballPath := path.Join(filesystem.StateDir, "recovery_db.tar.gz")
	unpackDir := path.Join(filesystem.StateDir, "recovery_db")

	// Determine if the recovery tarball exists
	if _, err := os.Stat(tarballPath); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	logger.Warn("Recovery tarball located; attempting DB recovery", logger.Ctx{"tarball": tarballPath})

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
	nodeInfo, err := dumpYamlNodeStore(clusterYamlPath)
	if err != nil {
		return err
	}

	// And update the local trust store with any changed addresses
	existingMembers, err := GetLocalClusterMembers(filesystem)
	if err != nil {
		return err
	}

	for _, member := range existingMembers {
		for _, nodeInfo := range nodeInfo {
			if member.DqliteID == nodeInfo.ID {
				member.Address = nodeInfo.Address
			}
		}
	}

	fmt.Println(existingMembers)

	err = UpdateTrustStore(filesystem.TrustDir, existingMembers)
	if err != nil {
		return err
	}

	// use the local info.yaml so that the dqlite ID is preserved on each
	// cluster member
	localInfoYamlPath := path.Join(filesystem.DatabaseDir, "info.yaml")
	recoveryInfoYamlPath := path.Join(unpackDir, "info.yaml")

	infoYaml, err := os.ReadFile(localInfoYamlPath)
	if err != nil {
		return err
	}

	err = os.WriteFile(recoveryInfoYamlPath, infoYaml, 0o664)
	if err != nil {
		return err
	}

	//FIXME: Take a DB backup

	// Now that we're as sure as we can be that the recovery DB is valid, we can
	// replace the existing DB
	err = os.RemoveAll(filesystem.DatabaseDir)
	if err != nil {
		return err
	}

	err = os.Rename(unpackDir, filesystem.DatabaseDir)
	if err != nil {
		return err
	}

	// Prevent the database being restored again after subsequent restarts
	err = os.Remove(tarballPath)
	if err != nil {
		return err
	}

	return nil
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

	err = os.MkdirAll(destRoot, 0o755)
	if err != nil {
		return err
	}

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
			return err
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
