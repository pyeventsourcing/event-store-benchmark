# Setup for running benchmarks on AWS

## Install AWS cli

On MacOS:

    brew install awscli

On Linux:

    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install

## Run the login command

In your terminal, simply run:

    aws login

## Install the session manager plugin

The AWS CLI needs a plugin to handle the live log tailing.

On macOS (Intel or Apple Silicon):

    curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac/sessionmanager-bundle.zip" -o "sessionmanager-bundle.zip"
    unzip sessionmanager-bundle.zip
    sudo ./sessionmanager-bundle/install -i /usr/local/sessionmanagerplugin -b /usr/local/bin/session-manager-plugin

On Linux (Ubuntu/Debian):

    curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" -o "session-manager-plugin.deb"
    sudo dpkg -i session-manager-plugin.deb

## Create the S3 bucket

The scripts assume a bucket named `esb-benchmark-results` exists. S3 bucket names must be globally
unique across all of AWS, so you will need to add a unique suffix (like your name or company).

    # Add suffix with something unique to you
    export MY_BUCKET="esb-benchmark-results"
    aws s3 mb s3://$MY_BUCKET

*Important:* You must now update the `S3_BUCKET` variable in both `aws/setup-iam.sh` and `aws/userdata.template.sh`
to match this new bucket name.

## Run the IAM setup script

    chmod +x aws/*.sh
    ./aws/setup-iam.sh

## Launch benchmark matrix

After setting everything up, you are ready to launch the benchmark matrix.

    ./aws/lauch-matrix.sh

## Fetch results

Sync the results bucket locally (also runs `make report`).

    ./aws/fetch-results.sh

Open `./results/index.html` in your browser.