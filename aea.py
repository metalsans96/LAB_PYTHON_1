import ray
import time

# Ініціалізуємо Ray
ray.init()

# Функція для обчислення суми списку чисел
def calculate_sum(numbers):
    return sum(numbers)

if __name__ == "__main__":
    # Генеруємо великий список чисел для обчислення суми
    numbers = list(range(1, 1000001))

    # Розбиваємо список на два підсписки для паралельного обчислення
    midpoint = len(numbers) // 2
    part1 = numbers[:midpoint]
    part2 = numbers[midpoint:]

    start_time = time.time()

    # Створюємо дві паралельні задачі для обчислення суми
    result1 = ray.remote(calculate_sum).remote(part1)
    result2 = ray.remote(calculate_sum).remote(part2)

    # Очікуємо на результати
    sum1 = ray.get(result1)
    sum2 = ray.get(result2)

    # Обчислюємо загальну суму
    total_sum = sum1 + sum2

    end_time = time.time()

    print(f"Загальна сума: {total_sum}")
    print(f"Час виконання: {end_time - start_time:.2f} секунд")

    # Завершуємо роботу з Ray
    ray.shutdown()