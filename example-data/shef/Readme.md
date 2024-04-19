-- Notes from John R on MT_Mesonet.shef
: Soil temperature (F at 2 inch depth) .E ASAM8 240419 Z DH0800/DUE/TBIRZ/DIH1 .E1 2.0372/ 2.0360/ 2.0351/ 2.0343/ 2.0338/ 2.0334/ 2.0340 == 3.72, 3.60, 3.51, 3.43, 3.38, 3.34, 3.40
: Soil moisture (pct at 4 inch depth) .E ASAM8 240419 Z DH0800/MVIRZ/DIH1 .E1 4.0250/ 4.0250/ 4.0251/ 4.0251/ 4.0250/ 4.0250/ 4.0249 = 2.50, 2.50, 2.51, 2.51, 2.50, 2.49
: Soil temperature (F at 4 inch depth) .E ASAM8 240419 Z DH0800/DUE/TBIRZ/DIH1 .E1 4.0459/ 4.0450/ 4.0441/ 4.0433/ 4.0428/ 4.0421/ 4.0415  4.59, 4.41, 4.33, 4.28, 4.21, 4.15 And so on.

Bug: What the current software does is take the values without the decimal shift and then truncates to a wrong value. So on the 4” the data was becoming: 4.15, 4.05, 4.04, 4.04, and so on. At least that is what I recalled it doing by misunderstanding the vector data.




Example: (From the shef manual)
HQ = Distance from a ground reference point to the river’s edge to estimate stage (value units are feet to river’s edge for a specific marker number) 
MD = Dielectric constant at depth (value units are dimensionless dielectric constant at depth in the soil in inches) 
MN = Soil salinity at depth (units are grams per liter of salinity at depth in the soil in inches) 
MS = Soil moisture amount at depth (value units are inches of soil moisture amount at depth in the soil in inches) 
MV = Percent water volume at depth (value units are percent soil moisture volume at depth in the soil in inches) 
NO = Gate opening for a specific gate (value units are feet of lock/dam gate opening for a specific lock/dam gate number) 
ST = Snow temperature at depth measured from ground (value units are degrees Fahrenheit at depth in the snow in inches measured up from the soil surface into the snow) 
TB = Temperature in bare soil at depth (value units are degrees Fahrenheit at depth in the soil in inches) 
TE = Air temperature at elevation above MSL (value units are degrees Fahrenheit at elevation above MSL in feet) 
TV = Temperature in vegetated soil at depth (value units are degrees Fahrenheit at depth in the soil in inches)
