import md5

file= raw_input('input the file name you wanna scan: ')

print file

filetoken=open(file)

filestring=filetoken.read()

m= md5.new()

m.update(filestring)

result=m.digest()

print result.encode("hex")