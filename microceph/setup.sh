# Start here snap isn't installed
sudo apt update
sudo apt install snapd

# Install MicroCeph
sudo snap install microceph

# Stop autoupdates
sudo snap refresh --hold microceph

# Create a single-node Ceph cluster with three OSDs
sudo microceph cluster bootstrap
sudo microceph disk add loop,4G,3

# Verify status
sudo microceph status

# Enable the S3 gateway on port 8080
sudo microceph enable rgw --port 8080

# Create an S3 user then copy generated keys
sudo microceph.radosgw-admin user create --uid=demo-user --display-name="demo-user"
