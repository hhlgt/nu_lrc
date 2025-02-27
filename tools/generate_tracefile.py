import random
import math
import sys
import os

def generate_tracefile(testtype, stripe_number):
    folder_path = './../tracefile/'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    line_number = stripe_number
    filename = folder_path + str(int(testtype)) + '_test' + '.txt'
    if testtype == 0:
        line_number = 1
    with open(filename, 'w') as f:
        for i in range(line_number):
            n = random.randint(2, 3)
            k = []
            for ii in range(n):
                k_i = random.randint(2, 6)
                k.append(k_i)
            object_sizes = []
            for ki in k:
                object_sizes.append(ki)
            w = []
            for ii in range(n):
                w_i = random.randint(1, 20)
                w.append(w_i)
            for ii in range(len(w)):
                para = '(' + str(object_sizes[ii]) + ',' + str(w[ii]) + ')'
                f.write(para)
                if ii != len(w) - 1:
                    f.write(',')
            f.write('\n')
    f.close()

def random_split(k, n, avg):
    result = []
    lower = max(math.floor(3 * avg / 4), 1)
    upper = math.floor(4 * avg / 3)
    last_point = 0
    for i in range(n):
        split_point = int(random.uniform(lower, upper))
        ki = split_point - last_point
        lower = split_point + 1
        upper += avg
        if i == n - 1:
            ki = k - last_point
        result.append(ki)
        last_point = split_point
    return result

def random_intialize_by_split(K, R, g, rate, stripe_number):
    folder_path = './../tracefile/'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    l = int((K + g + R - 1) / R)
    x = round(float(K + g + l) / float(K), 3)
    filename = folder_path + str(x) + '_' + str(K) + '_' + str(R) + '_' + str(g) + '_' + str(rate) + '_' + str(stripe_number) + '.txt'    
    # n_max = math.floor(K / 4)
    # n = random.randint(2, min(n_max, 10))
    n = 5
    fl_avg = K / n
    with open(filename, 'w') as f:
        for i in range(stripe_num):
            x_g = str(x) + ',' + str(g) + ','
            f.write(x_g)
            k = random_split(K, n, fl_avg)
            random.shuffle(k)
            w = []
            if rate == 0:
                for i in range(n):  # random file distribution
                    w_i = random.randint(1, 20)
                    w.append(w_i)
            else:   # rate of hot file
                hot_file_num = math.ceil(n * rate)
                for i in range(n):
                    w_i = 0
                    if i < hot_file_num:
                        w_i = random.randint(16, 20)
                    else:
                        w_i = random.randint(1, 5)
                    w.append(w_i)
            random.shuffle(w)
            for ii in range(len(w)):
                para = '(' + str(k[ii]) + ',' + str(w[ii]) + ')'
                f.write(para)
                if ii != len(w) - 1:
                    f.write(',')
            f.write('\n')
    f.close()

def generate_random_workload(x_low, x_up, rate, stripe_number):
    folder_path = './../tracefile/'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    filename = folder_path + 'wl_' + str(x_low) + '-' + str(x_up) + '_' + str(rate) + '_' + str(stripe_number) + '.txt'
    with open(filename, 'w') as f:
        for i in range(stripe_num):
            factor = random.randint(3, 30)
            K = 4 * factor
            g = random.randint(2, 4)
            x_0 = float((g + K + 2) / K)
            x_0 = max(x_0, x_low)
            x = round(random.uniform(x_0, x_up), 3)
            if x < x_0:
                x = x_0
            x_g = str(x) + ',' + str(g) + ','
            f.write(x_g)
            n = 5
            fl_avg = K / n
            k = random_split(K, n, fl_avg)
            random.shuffle(k)
            w = []
            if rate == 0:
                for i in range(n):  # random file distribution
                    w_i = random.randint(1, 20)
                    w.append(w_i)
            else:   # rate of hot file
                hot_file_num = math.ceil(n * rate)
                for i in range(n):
                    w_i = 0
                    if i < hot_file_num:
                        w_i = random.randint(16, 20)
                    else:
                        w_i = random.randint(1, 5)
                    w.append(w_i)
            random.shuffle(w)
            for ii in range(len(w)):
                para = '(' + str(k[ii]) + ',' + str(w[ii]) + ')'
                f.write(para)
                if ii != len(w) - 1:
                    f.write(',')
            f.write('\n')
    f.close()


if len(sys.argv) == 3 or len(sys.argv) == 6 or len(sys.argv) == 7:
    func = int(sys.argv[1])
    stripe_num = int(sys.argv[2])
    if func == 1 and len(sys.argv) == 3:
        generate_tracefile(1, stripe_num)
    elif func == 2 and len(sys.argv) == 7:
        rate = float(sys.argv[3])
        K = int(sys.argv[4])
        R = int(sys.argv[5])
        g = int(sys.argv[6])
        random_intialize_by_split(K, R, g, rate, stripe_num)
    elif func == 3 and len(sys.argv) == 6:
        rate = float(sys.argv[3])
        x_low = float(sys.argv[4])
        x_up = float(sys.argv[5])
        generate_random_workload(x_low, x_up, rate, stripe_num)
else:
    print('Invalid arguments! Usage. ')
    print('->$ python generate_tracefile.py 1 stripe_num')
    print('->$ python generate_tracefile.py 2 stripe_num rate K R g')
    print('->$ python generate_tracefile.py 3 stripe_num rate x_low x_up')
