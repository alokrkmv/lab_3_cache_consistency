

file_name = "/home/asit/Documents/Saurabh/677/lab_3_cache_consistency/src/cache_sync/outputs/total_output.txt"


for text in ['trader0', 'trader1', 'trader2']:

    out = [ line for line in open(file_name) if text in line.lower()]
    f = open("/home/asit/Documents/Saurabh/677/lab_3_cache_consistency/src/cache_sync/outputs/{}.txt".format(text), "a")
    for line in out:
        f.write(line)
    f.close()


out = [ line for line in open(file_name) if ('is unavailable' in line.lower()) or ('is shipped from inventory' in line.lower())]
f = open("/home/asit/Documents/Saurabh/677/lab_3_cache_consistency/src/cache_sync/outputs/database_op.txt", "a")
for line in out:
    f.write(line)
f.close()