ssh -i $keyValue ec2-user@$PUBLIC_DNS "/tmp/server.sh < /dev/null > /tmp/mymasterlog 2>&1 &"
