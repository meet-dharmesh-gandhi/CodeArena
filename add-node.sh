LAB_PCS=("10.20.14.131" "10.20.14.118" ) # "10.20.14.124" "10.20.14.108")
DEST_PATH="~/Desktop/CodeArena/node"

for IP in "${LAB_PCS[@]}"; do
    echo "Syncing to $IP..."
    rsync -avz -e ssh ./node Lab208@$IP:$DEST_PATH
done

echo "Now go to the PC and run these commands:"
echo "nano ~/.bashrc"
echo "In the last line, write this:"
echo "export PATH=/home/Lab208/Desktop/CodeArena/node/node/bin:\$PATH"
echo "Now run: source ~/.bashrc"
echo "Done, now 'npm -v' and 'node -v' will work"
