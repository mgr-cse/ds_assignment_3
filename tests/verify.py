
# find total messages produced
prod_message = 0
for i in range(1, 6):
    with open('tests/log/P-' + str(i) + '.txt', 'r') as file:
        prod_message += len(file.readlines())

# find total messages consumed
cons_message = 0
for i in range(1, 4):
    with open('tests/log/C-1_T-' + str(i) + '.txt', 'r') as file:
        cons_message += len(file.readlines())

if cons_message == prod_message:
    print('+++++ Test pass: C-1 messages = Sum of producer messages')
else: print('----- Test fail C-1 messages != Sum of producer messages')

for topic in ['T-1', 'T-3']:
    with open('tests/log/C-1_' + topic + '.txt') as c1:
        with open ('tests/log/C-2_' + topic + '.txt') as c2:
            if c1.read() == c2.read():
                print('+++++ Test pass: C-1 = C-2 for', topic)
            else: print('----- Test fail C-1 != C-2 for', topic)
    
    with open('tests/log/C-1_' + topic + '.txt') as c1:
        with open ('tests/log/C-3_' + topic + '.txt') as c2:
            if c1.read() == c2.read():
                print('+++++ Test pass: C-1 = C-3 for', topic)
            else: print('----- Test fail C-1 != C-3 for', topic)
    
