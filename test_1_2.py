
### 1

# Function to find anagrams of a given word
def anagrams(word):
    # Read words from the file "WORD.lst" and remove trailing spaces
    words = [w.rstrip() for w in open('data/WORD.lst')]

    anagrams = []

    # Check for anagrams
    for i in words:
        if sorted(i) == sorted(word) and i != word:
            anagrams.append(i)

    return anagrams

if __name__ == "__main__":
    # Example usages
    print(anagrams("train"))
    print('--')
    print(anagrams('drive'))
    print('--')
    print(anagrams('python'))
    print('--')