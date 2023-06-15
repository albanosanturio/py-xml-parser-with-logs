import xml.etree.cElementTree as Xet # For parsing XML element tree

import xml.etree.ElementTree as ET
tree = Xet.parse('./in/inputfile4.xml')
root = tree.getroot()

print("\n>Printing roots:")
print(root[0].tag)
print(root[1].tag)
print(root[2].tag)
print(root[3].tag)

print("\n>Printing childs")
for child in root:
    print(child.tag, child.attrib)


print("\n>Printing childs of Channels")
for child in root[3]:
    print(child.tag, child.attrib)

print("\n>Printing lenght of childs of Channels")
print(type(root[3]))
print(len(root[3]))

print("\n>Printing one chanel")
for child in root[3][0]:
    print(child.tag, child.attrib)



print("\n>Printing one chanel")
for channelid in root[3].iter('ChannelID'):
    print(channelid.attrib)

#context = Xet.iterparse(fullname, events=("start", "end"))


