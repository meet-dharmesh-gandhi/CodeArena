sudo docker system df

if [[ $# -gt 0 ]]; then
    case "$1" in
        [Yy]*)
            sudo docker system prune -a --volumes
            ans="0"
            ;;
        *)
            read -p "Clear docker? [y/n]: " ans
    esac
else
    read -p "Clear docker? [y/n]: " ans
fi

case ans in
    [Yy]*)
        sudo docker system prune -a --volumes
        echo "docker cache removed"
        ;;
esac
