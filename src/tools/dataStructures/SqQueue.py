# 循环队列
class SqQueue(object):
    def __init__(self, maxsize):
        self.queue = [None] * (maxsize + 1)
        self.maxsize = maxsize + 1
        self.front = 0
        self.rear = 0

    # 返回当前队列的长度
    def Size(self) -> int:
        return (self.rear - self.front + self.maxsize) % self.maxsize

    # 如果队列未满，则在队尾插入元素，时间复杂度O(1)
    def enQueue(self, data):
        if (self.rear + 1) % self.maxsize == self.front:
            raise Exception("The queue is full!")
        else:
            self.queue[self.rear] = data
            # self.queue.insert(self.rear,data)
            self.rear = (self.rear + 1) % self.maxsize

    # 如果队列不为空，则删除队头的元素,时间复杂度O(1)
    def deQueue(self):
        if self.rear == self.front:
            raise Exception("The queue is empty!")
        else:
            data = self.queue[self.front]
            self.front = (self.front + 1) % self.maxsize
            return data

    # 输出队列中的元素
    def ShowQueue(self):
        index = self.front
        while index != self.rear:
            print(self.queue[index], end=" ")
            index = (index + 1) % self.maxsize
        print()

    def IsFull(self):
        if (self.rear + 1) % self.maxsize == self.front:
            return True
        else:
            return False

    def isEmpty(self):
        if self.rear == self.front:
            return True
        else:
            return False


# 测试程序
if __name__ == "__main__":
    # 建立大小为15的循环队列
    q = SqQueue(15)
    print(f"是空的吗？{q.isEmpty()}")
    # 0~11入队列
    for i in range(12):
        q.enQueue(i)
    q.ShowQueue()
    # 删除队头的5个元素：0~4
    for i in range(5):
        q.deQueue()
    q.ShowQueue()
    # 从队尾增加8个元素：0~7
    for i in range(8):
        q.enQueue(i)
    print(f"是满的吗？{q.IsFull()}")
    q.ShowQueue()
