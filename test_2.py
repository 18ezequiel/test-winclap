## 2

# Function to process numbers and find "sheep"
def sheep_function(file_name):
    def process_number(number):
        if 0 <= number <= 200:
            if number == 0:
                return ('INSOMNIA', 0)

            original_number = number
            digits_seen = set()
            count = 1

            # Loop to check the length of the set of digits seen
            while len(digits_seen) < 10: 
                new_number = original_number * count
                count += 1
                digits_seen.update(str(new_number))

                if count > 1000:
                    return ('INSOMNIA', 0)

            return (new_number, count - 1)
        
        # Return out of range message
        else: 
            return ('Number out of range (0 ≤ N ≤ 200)', 0)

    # Read input numbers from the file
    with open(file_name) as file:
        random_numbers = [int(line.strip()) for line in file]

    results = [process_number(num) for num in random_numbers]
    formatted_results = []

    # Format and print output results
    for i, result in enumerate(results, start=1):
        if result[0] == 'INSOMNIA':
            formatted_results.append(f'Case #{i}: INSOMNIA')
        else:
            formatted_results.append(f'Case #{i}: {result[0]}')

    for result in formatted_results:
        print(result)

# Call the sheep_function with input from the file "c-input.in"
sheep_function('data/c-input.in')