#!/bin/sh

# Decrypt the file
mkdir $HOME/secrets
# --batch to prevent interactive command
# --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt --passphrase="$SECRET_PASSPHRASE_CREDS" \
--output $HOME/secrets/secret_cred.json secret_cred.json.gpg