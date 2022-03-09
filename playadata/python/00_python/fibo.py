
def fib(n):
    a, b = 0, 1
    
    while b<n:            # 조건이 true인 경우에만 실행되는 반복문
        print(b, end=' ')
        a, b = b, a+b    
