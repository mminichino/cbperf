Url=${1:-127.0.0.1:9499}
Site=http://$Url/query/service
while read line;
do
sql=$line
echo curl -u Administrator:password -v $Site  -d statement="$sql"
curl -u Administrator:password -v $Site  -d statement="$sql"
done
