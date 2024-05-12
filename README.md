# Credits
1. Adding TCP:
   1. https://betterprogramming.pub/build-a-tcp-connection-pool-from-scratch-with-go-d7747023fe14
   2. https://opensource.com/article/18/5/building-concurrent-tcp-server-go

# Benchmarks   
## Initial LSM Index
SSTable size - `50B`  
Sparse Index size - `10 Keys`  
Sparse Index search - `Linear Search`  
Tests count - `10,000`  

Tests took `10m2.598477625s`

READ NEW - 2523  
WRITE NEW - 2474  
READ OLD - 2501  
WRITE OLD - 2502  

## Binary Search on Sparse Indexes
SSTable size - `5KB`  
Sparse Index size - `100 Keys`  
Sparse Index search - `Binary Search`  
Tests count - `10,000`  

Tests took `30.6950765s`  
Data size `454.507 kb`

READ NEW - 2490  
WRITE NEW - 2461  
READ OLD - 2550  
WRITE OLD - 2499  

## Compress & Merge
SSTable size - `5KB`  
Sparse Index size - `100 Keys`  
Sparse Index search - `Binary Search`  
Tests count - `10,000`  

Tests took `37.880961042s`  
Data size `465.564 kb`

READ NEW - 2489  
WRITE NEW - 2570  
READ OLD - 2450  
WRITE OLD - 2491  

## Compress & Merge Increased Test Count
SSTable size - `5KB`  
Sparse Index size - `100 Keys`  
Sparse Index search - `Binary Search`  
Tests count - `1,00,000`

Tests took `37.880961042s`  
Data size `465.564 kb`

READ NEW - 2489  
WRITE NEW - 2570  
READ OLD - 2450  
WRITE OLD - 2491  
