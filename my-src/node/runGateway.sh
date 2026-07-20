cd ~/"Desktop/Code Files/CodeArena/my-src/node/"
node-gyp configure build
read -p "proceed? [Y/N]: " ans
case "$ans" in
    [yY][eE][sS]|[yY] | "")
        echo "You chose Yes. Proceeding..."
        ;;
    *)
        echo "You chose No (or hit Enter). Aborting..."
        exit
        ;;
esac
echo
echo "______________________________________________________________________________"
echo "                                  OUTPUT"
echo "______________________________________________________________________________"
echo
npm run dev
