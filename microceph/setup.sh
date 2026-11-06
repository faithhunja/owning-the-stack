# Start here if snap isn't installed
sudo apt update
sudo apt install snapd

# Install MicroCeph
sudo snap install microceph

# Stop autoupdates (in case something breaks mid-demo)
sudo snap refresh --hold microceph

# Create a single-node MicroCeph cluster with three OSDs
sudo microceph cluster bootstrap
sudo microceph disk add loop,4G,3

# Verify status
sudo microceph status

# Enable the S3 gateway on port 8080
sudo microceph enable rgw --port 8080

# Create an S3 user then copy generated keys
sudo microceph.radosgw-admin user create --uid=demo-user --display-name="demo-user"

#### For when you start a new session:
# Check status of snap services
sudo snap services microceph

# Start the daemon
sudo snap start microceph.daemon

# Start the RGW (S3 gateway)
sudo snap start microceph.rgw

# Verify MicroCeph is back online
sudo microceph status

# List the files in the bucket (owning-the-stack)
sudo microceph.radosgw-admin bucket list --bucket=owning-the-stack
