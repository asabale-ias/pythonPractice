class MyNumbers:
    def __iter__(self):
        self.a = 1
        print("i")
        return self

    def __next__(self):
        x = self.a
        self.a += 1
        print("n")
        return x

myclass = MyNumbers()
myiter = iter(myclass)

print(next(myiter))
print(next(myiter))
print(next(myiter))
print(next(myiter))
print(next(myiter))