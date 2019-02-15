''' Proof of concept for reading in file and dumping it into a Python dict. For this to be paralellizable something will probably
need to be done at the DB end of things.
This is expecting a .tsv file in the next directory up, with user_id at index 0, star rating at 7 and review body at 13. '''

import os

# so my local paths work
os.chdir(os.getcwd() + '/test')
print(os.getcwd())

users = dict()

with open('test_input.tsv') as camin:
    line = camin.readline()
    for line in camin:
        line = line.split('\t')
        # For each user, store how many reviews they gave, the star rating of each, and how many words are in each.
        try:
            users[line[1]][0] += 1
            users[line[1]][1] += int(line[7])
            users[line[1]][2] += line[13].count(' ')
        except:
            users[line[1]] = [1, int(line[7]), line[13].count(' ')]

with open(input_filename) as input_stream:
    reader = DictReader(input_filename, delimiter='\t')
    for row in reader:
        for index in row:
            print( index )

multiple = 0 # Will tell me how many will be eliminated.
             # In this case, two or more reviews that are 5 stars and have five or fewer words.
for user in users:
    if users[user][0] > 1 and users[user][1] / users[user][0] == 5 and users[user][2] / users[user][0] < 6:
        multiple += 1
        print(user, users[user][0], users[user][1] / users[user][0], users[user][2] / users[user][0])

print( len(users), multiple )
