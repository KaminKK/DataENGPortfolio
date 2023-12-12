def find_closest(Q, j):
    closest_value = None
    min_distance = float('inf')

    for value in Q:
        distance = abs(value - j)
        
        if distance < min_distance or (distance == min_distance and value < closest_value):
            min_distance = distance
            closest_value = value

    return closest_value

Q = [1, -1, -5, 2, 4, -2, 1]
j = 3
result = find_closest(Q, j)
print(result)