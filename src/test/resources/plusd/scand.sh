#!/bin/bash
for file in ` ls *.d `
do
echo "-----------------------------------------------"
echo $file
echo "-----------------------------------------------" 
cat $file
done
