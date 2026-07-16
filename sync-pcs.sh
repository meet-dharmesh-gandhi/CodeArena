#!/bin/bash

# List your lab PC IPs here
LAB_PCS=("10.20.14.131" "10.20.14.124" "10.20.14.118" "10.20.14.108")
DEST_PATH="~/Desktop/CodeArena/"
EXCLUDES="--exclude=.git/ --exclude=*.o --exclude=node_modules/ --exclude=.vscode/ --exclude=Tests-ignore/ --exclude=*.docx --exclude=build/"

for IP in "${LAB_PCS[@]}"; do
    echo "Syncing to $IP..."
    rsync -avz $EXCLUDES --delete ./ "Lab208@$IP:$DEST_PATH"
done

echo "Done!"
