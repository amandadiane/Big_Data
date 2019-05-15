
from pprint import pprint as pp
cm_list = [[0]*10 for i in range(10)]
for (i, (e,p)) in enumerate(zip(expected, predicted)):
    cm_list[e][p] += 1
    if (e==5) and p!=5:
        print(e,p)
        pp(test_img[i])
