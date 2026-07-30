#!/bin/bash

# List your lab PC IPs here
LAB_PCS=("10.20.16.78" "10.20.16.50" "10.20.16.52" "10.20.16.83") #"10.20.14.131" "10.20.14.118" "10.20.14.124" "10.20.14.108")
DEST_PATH="~/Desktop/CodeArena/"
EXCLUDES="--exclude=.git/ --exclude=.vscode/ --exclude=Tests-ignore/ --exclude=*.docx --exclude=*docker* --exclude=logs*.txt --exclude=Dockerfile"

for IP in "${LAB_PCS[@]}"; do
    echo "Syncing to $IP..."
    rsync -avz $EXCLUDES --delete ./ "Lab205@$IP:$DEST_PATH"
done

echo "Done!"

echo "Now go to the PC and run these commands:"
echo "nano ~/.bashrc"
echo "In the last line, write this:"
echo "export PATH=/home/Lab208/Desktop/CodeArena/node/node/bin:\$PATH"
echo "Now run: source ~/.bashrc"
echo "Done, now 'npm -v' and 'node -v' will work"
