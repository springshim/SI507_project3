# b = 'bars sourceregion=Africa ratings top=5'
# a = b.split()
# print(a[-1][4:])

# if 'ratings' in a:
# 	print('Hi')


# c = 13.54435325
# c = float("{0:.2f}".format(c))
# print(c)



aString = "bars ratings"
c = aString.split()
if (c[1].startswith ('baby')) or (c[1].startswith ('jude')) or (c[1].startswith ('ratings')) :
	print("ratings here")
else:
	print("hhh")