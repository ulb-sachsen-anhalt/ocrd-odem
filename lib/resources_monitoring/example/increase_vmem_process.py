import time

print('increase_vmem_process start')
random_string = ' '
for i in range(60):
    print('dm')
    random_string += ' ' * 512000000
    time.sleep(2)
print('increase_vmem_process stop')
