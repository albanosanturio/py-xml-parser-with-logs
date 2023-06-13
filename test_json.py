import json

# Opening JSON file
f = open('config.json')
  
# returns JSON object as 
# a dictionary
data = json.load(f)
input_path = data['paths']['input_path']
output_path = data['paths']['output_path']
error_path = data['paths']['error_path']
log_path = data['paths']['log_path']

# Closing file
f.close()

print(input_path)
print(output_path)
print(error_path)
print(log_path)

print(type(input_path))

#input_path = "C:\\Users\\U355445\\Documents\\AMI Filter\\NY_in"
#output_path = "C:\\Users\\U355445\\Documents\\AMI Filter\\test"
#error_path = '\\\clornas01\\Uplight\\NY\\errors'
#log_path = '\\\clornas01\\Uplight\\NY\\logs'