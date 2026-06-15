toCompile=("Node" "Monitor")
deps="heartbeats.c socket.c consensus.c packets.c"

for file in "${toCompile[@]}"; do
    echo "Compiling $file..."
    gcc $file.c $deps -o $file
done

echo "Done!"
