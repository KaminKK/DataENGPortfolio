## **Question**

Suppose you are given a list of Q 1D points. Write code to return the value in Q that is the closest to value j.
 If two values are equally close to j, return the smaller value. 
   
**Example**:   

 
```
Q = [1, -1, -5, 2, 4, -2, 1]  

j = 3

#Output: 2
``` 



## **Solution**

We iterate through the elements of the list Q, calculate the absolute difference between each element and j, and keep track of the closest value and its corresponding distance. 

If we find a value that is closer or equally close but smaller, we update the closest_value and min_distance. Finally, we return the closest_value.



