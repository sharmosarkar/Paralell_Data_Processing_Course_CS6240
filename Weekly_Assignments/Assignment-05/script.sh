# Scipt that runs to give the count of missed connections per carrier based on our analysis
# Author : Sarita and Sharmo
# Co-author : Yogiraj and Ashish 
# Redirecting the output to a text file
cat output > output.txt
mkdir output1
sed 's/\t/,/g' output.txt > output1/output1.txt
mkdir output1/out
#mkdir out
# Split data as per Carrier and year data received from reducer
awk -F, '{print > "output1/"$1$2".txt"}' output1/output1.txt
rm output1/output1.txt
# Replace tab  with spaces
for f in output1/*.txt
do
filename=$(basename $f)
sed 's/,/ /g' $f > output1/out/$filename
done

# Count the no. of missed connection flights.
for f in output1/out/*.txt
do
fname=$(basename $f)
echo -e "Format for Display : FileName TotalConnections MissedConnections Percentage">> output1/out/result.txt
echo $fname >> output1/out/result.txt
totalConnections=$(awk '{s+=$4}END{print s}' $f)
missedConnections=$(awk '{s+=$3}END{print s}' $f)
# iterating for results
echo $totalConnections >> output1/out/result.txt
echo $missedConnections >> output1/out/result.txt
echo $(($missedConnections * 100 / $totalConnections))\% >> output1/out/result.txt
done
#sed -i ':a;N;$!ba;s/\n/\t/g' output1/out/result.txt
