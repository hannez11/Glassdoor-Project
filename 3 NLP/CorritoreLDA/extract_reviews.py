#this file extracts only the reviewid and text columns from
#reviews_UCBerkeley.csv and outputs to /tmp/
import csv, sys

csv.field_size_limit(sys.maxsize)

#extract the rows we need
header = ['reviewID','orgID','datetime','status','cb_1','cb_2','cb_3','naics','name','shortName','sectorName','industryName','pro','con','feedback']

masterFile = open("/project/def-mcorrito/mcorrito/reviews_UCBerkeley.csv",'rU')
newFile = open("/project/def-mcorrito/mcorrito/gd_contractors/temp_data/extract.csv",'w')
write = csv.writer(newFile)
write.writerow(header)

filtered = (line.replace('\0',' ') for line in masterFile)
filtered = (line.replace('\r\n','. ') for line in filtered)
filtered = (line.replace('\n','. ') for line in filtered)
read = csv.reader(filtered,delimiter=",")

next(read)

for row in read:
    if len(row)==57:
        dataRow = [row[0],row[1],row[16],row[20],row[27],row[28],row[29],row[43],row[44],row[45],row[49],row[50],row[54],row[55],row[56]]
        write.writerow(dataRow)

masterFile.close()
newFile.close()
    


    


