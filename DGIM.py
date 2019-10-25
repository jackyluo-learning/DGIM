import math


def UpdateContainer(container, bucketlist):
    itemlist = []
    for bucket in bucketlist:
        itemlist = container[bucket]
        # print(bucket,itemlist)
        if len(itemlist) > 2:
            start = container[bucket][0][0]
            container[bucket].pop(0)
            end = container[bucket][0][1]
            container[bucket].pop(0)
            if bucket != bucketlist[-1]:
                container[bucket * 2].append((start, end))
        else:
            break


def OutputResult(container, bucketlist, wsize, lasttime):
    count = 0
    wstart = lasttime - wsize + 1
    for bucket in bucketlist:
        if len(container[bucket]) == 0:
            continue
        # if bucket != 1:
        if wstart >= container[bucket][0][0] and wstart < container[bucket][-1][1]:
            if wstart >= container[bucket][-1][0]:
                count += 0.5 * bucket
                continue
            if wstart >= container[bucket][0][0]:
                count += 1.5 * bucket
                continue
        if wstart <= container[bucket][0][0]:
            count += bucket * len(container[bucket])
            continue

    return count


def BruteForce(stream, wsize):
    count = 0
    for index, item in enumerate(stream):
        if index >= len(stream) - wsize:
            if item == '1':
                count += 1
    return count


if __name__ == '__main__':
    file = open("stream_data.txt")
    content = file.readlines()
    # print(content)
    for line in content:
        split_line = line.strip()
        split_line = line.split('\t')
        if (split_line[-1] != '1' or split_line[-1] != '0'):
            split_line = split_line[0:-1]
    print("read stream size:", split_line.__len__())
    print("stream content:", split_line)

    container = {}
    windowsize = 1000
    timestamp = 0

    bucketsnum = int(math.log(windowsize, 2)) + 1
    bucketlist = list()
    # initialize the container
    for i in range(bucketsnum):
        bucket = int(math.pow(2, i))
        bucketlist.append(bucket)
        container[bucket] = list()

    print("bucket: ", container)

    timestamp = 0
    itemlist = []
    # split_line = ['1','0','0','1','0','1','0','1','0','0']
    for char in split_line:
        if char == '1':
            itemlist = container[1]
            itemlist.append((timestamp, timestamp))
            container[1] = itemlist
            UpdateContainer(container, bucketlist)
        timestamp += 1

    print("bucket: ", container)
    print("Estimation: ", OutputResult(container, bucketlist, windowsize, len(split_line) - 1))
    print("Actual: ", BruteForce(split_line, windowsize))
