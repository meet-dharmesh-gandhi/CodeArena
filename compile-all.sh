cd ~/"Desktop/Code Files/CodeArena/my-src"

toCompile=("assigner"  "monitor" "worker" "empty")
deps="../include/constants.h ../include/async.c ../include/memory.c ../include/morph.c ../include/network.c ../include/print.c ../include/utils.c"

for file in "${toCompile[@]}"; do
    echo "Compiling $file..."
    gcc $file.c $deps -o $file
done

echo "Compiling gateway"

./node/runGateway.sh
