-- Notes from John R on  MT_Mesonet.shef

: Soil temperature (F at 2 inch depth)
.E ASAM8 240419 Z DH0800/DUE/TBIRZ/DIH1
.E1 2.0372/ 2.0360/ 2.0351/ 2.0343/ 2.0338/ 2.0334/ 2.0340  == 3.72, 3.60, 3.51, 3.43, 3.38, 3.34, 3.40

: Soil moisture (pct at 4 inch depth)
.E ASAM8 240419 Z DH0800/MVIRZ/DIH1
.E1 4.0250/ 4.0250/ 4.0251/ 4.0251/ 4.0250/ 4.0250/ 4.0249  = 2.50, 2.50, 2.51, 2.51, 2.50, 2.49

: Soil temperature (F at 4 inch depth)
.E ASAM8 240419 Z DH0800/DUE/TBIRZ/DIH1
.E1 4.0459/ 4.0450/ 4.0441/ 4.0433/ 4.0428/ 4.0421/ 4.0415   4.59, 4.41, 4.33, 4.28, 4.21, 4.15
And so on.

Bug: What the current software does is take the values without the decimal shift and then truncates to a wrong value.  So on the 4” the data was becoming: 4.15, 4.05, 4.04, 4.04,  and so on.
At least that is what I recalled it doing by misunderstanding the vector data.
