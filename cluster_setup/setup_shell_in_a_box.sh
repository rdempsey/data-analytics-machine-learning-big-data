#!/bin/bash

# Install dependencies
apt-get install git libssl-dev libpam0g-dev zlib1g-dev dh-autoreconf

# Close the repo and cd into it
git clone https://github.com/shellinabox/shellinabox.git && cd shellinabox

# Run autotools in the project directory
autoreconf -i

# Run configure and make to create the executable
./configure && make
