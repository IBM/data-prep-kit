# Install ClamAV with Homebrew
brew install clamav

# Copy and edit config files.
\cp -f $(brew --prefix)/etc/clamav/clamd.conf.sample $(brew --prefix)/etc/clamav/clamd.conf
sed -i '' -e 's/^Example/# Example/' $(brew --prefix)/etc/clamav/clamd.conf
echo "DatabaseDirectory /var/lib/clamav" >> $(brew --prefix)/etc/clamav/clamd.conf
echo "LocalSocket /var/run/clamav/clamd.ctl" >> $(brew --prefix)/etc/clamav/clamd.conf
\cp -f $(brew --prefix)/etc/clamav/freshclam.conf.sample $(brew --prefix)/etc/clamav/freshclam.conf
sed -i '' -e 's/^Example/# Example/' $(brew --prefix)/etc/clamav/freshclam.conf
echo "DatabaseDirectory /var/lib/clamav" >> $(brew --prefix)/etc/clamav/freshclam.conf

# Create a directory for a local unix socket
if [ ! -e /var/run/clamav ]; then
    sudo mkdir -p /var/run/clamav
fi
sudo chown $(id -u):$(id -g) /var/run/clamav

# Create a direcotry for a database of ClamAV
if [ ! -e /var/lib/clamav ]; then
    sudo mkdir -p /var/lib/clamav
fi
sudo chown $(id -u):$(id -g) /var/lib/clamav

# Update a database of ClamAV
freshclam