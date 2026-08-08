cd ~/Desktop/CodeArena

./src/empty &> output.txt & read -rsn 1 ans; kill $!

less -R output.txt
