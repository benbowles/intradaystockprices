
from collections import namedtuple
from scipy.ndimage import gaussian_filter
import math
from scipy import odr
import numpy as np

class Range(object):

    def __init__(self, order, lowerp, upperp, direction):
        self.order = order
        self.lowerp = lowerp
        self.upperp = upperp
        self.direction = direction

        self.results = {}

    def lowerN(self, N):
        return math.floor(self.lowerp * N)

    def upperN(self, N):
        return math.floor(self.upperp * N)

    def getstr(self):
        return f"[order={self.order}, lowerp={self.lowerp}, upperp={self.upperp}, direction={self.direction}]"

    def __str__(self):
        return self.getstr()

    def __repr__(self):
        return self.getstr()

    
class RangeSet(object):
    
    def __init__(self):
        self.ranges = self.get_ranges()
    
    # assumes working from the last item in series
    @staticmethod
    def get_ranges():
        fiveRange = [Range(*r) for r in [
            (0, 0.05, 0.10, "-"),
            (1, 0.15, 0.35, "+"),
            (2, 0.15, 0.35, "-"),
            (3, 0.15, 0.35, "+"),
            (4, 0.15, 0.35, "-"),
        ]]
        return fiveRange

    def __getitem__(self, arg):
        return self.ranges[arg]
    
    def __len__(self):
        return len(self.ranges)
    
def find_values5(long_values):
    i = 0
    rangeslist = []
    rangelen = 100
    while i < len(long_values):
        if i >= rangelen:
            ranges = RangeSet()
            tem_vals = long_values[(i - rangelen):i]
            result = look_for_up_down_pattern3(tem_vals, ranges, debug=False)
   
            if result:
                ranges.start = (i-rangelen)
                ranges.end = i
                rangeslist.append(ranges)
                i += 100
                continue
        i += 1
    return rangeslist
                
def get_slopes(vals):
    slopes = []
    for v in range(1, len(vals)):
        slopes.append(vals[v] - vals[v-1])
        
    return slopes



def update_range(current_range, iters_in_range, i, cur_range_i, raw_values, ranges):
    current_range.results['len_in_range'] = iters_in_range
    current_range.results['i_when_leave'] =  i
    if cur_range_i == 0:
        gain = raw_values[i] -  raw_values[0]
    else:
        gain = raw_values[i] -  raw_values[ranges[cur_range_i - 1].results['i_when_leave']]
    current_range.results['gain'] = gain
    current_range.results['overall_slope'] = gain / iters_in_range

    
def flatten(target):
    # Define a function (quadratic in our case)
    # to fit the data with.
    # odr initially assumes a linear function
    def target_function(p, x):
        m, c = p
        return m*x + c

    odr_model = odr.Model(target_function)

    data = odr.Data(range(len(target)), target)

    ordinal_distance_reg = odr.ODR(data, odr_model,
    beta0=[0.2, 1.])

    model_out = ordinal_distance_reg.run()
    return model_out

def look_for_up_down_pattern3(rawdata_o, ranges, debug=True):

    ranges.rawdata_o = rawdata_o
    rawdata_r = list(reversed(rawdata_o))
    
    ranges.fittedslope = np.polyfit(range(len(rawdata_o)), rawdata_o,  1)[0]
    rawdata_r_sm = gaussian_filter(rawdata_r, 6)
    ranges.residuals = np.absolute(np.array(rawdata_r).squeeze() - np.array(rawdata_r_sm).squeeze())
    ranges.residuals_mean = np.mean(ranges.residuals)
    
    model_out = flatten(rawdata_r_sm)
    rawdata_r_sm_res = rawdata_r_sm - np.poly1d(model_out.beta)(range(len(rawdata_r_sm)))
    ranges.coef = model_out.beta
    slopes_rawdata_r_sm_res = get_slopes(rawdata_r_sm_res)
    assert len(slopes_rawdata_r_sm_res) == len(rawdata_r_sm_res) - 1, 'len slopes is ' + str(len(slopes_rawdata_r_sm_res))
    
    success = True
    i = 0
    iters_in_range = 0
    cur_range_i = 0
    N = len(slopes_rawdata_r_sm_res)

    switched_inds = []
    ranges.slopes_rawdata_r_sm_res = slopes_rawdata_r_sm_res
    ranges.rawdata_r_sm_res = rawdata_r_sm_res
    while 1:

        if i == (len(slopes_rawdata_r_sm_res) - 1):
            if cur_range_i != (len(ranges) - 1):
                if debug:
                    print(
                        f"You got to end of list ( i = {i} however, it appears you didnt get all ranges. cur_range_i is {cur_range_i}"
                    )
                success = False
                break
            elif iters_in_range < current_range.lowerN(N):
                if debug:
                    print(
                        f"You got to end of list and to the last range, but you didnt get enough in last range.iters_in_range was {iters_in_range} and you were supposed to get {current_range.lowerN}"
                    )
                success = False
                break
            else:
                if debug:
                    print("success")
                update_range(current_range, iters_in_range, i, cur_range_i, rawdata_r_sm, ranges)
                break

        current_range = ranges[cur_range_i]
        current_slope = slopes_rawdata_r_sm_res[i]

        if not (
            (current_slope > 0 and current_range.direction == "+")
            or (current_slope < 0 and current_range.direction == "-")
        ):

            # means we got enough in last range
            if (
                iters_in_range > current_range.lowerN(N) and iters_in_range <= current_range.upperN(N)
            ):  
 
                if cur_range_i == (
                    len(ranges) - 1
                ):  # if 4 ranges, but is cur_range_i is 3, we're done
                    if debug:
                        print(
                            f"At {i}, You got to the end of the last range when you werent supposed  to on range {current_range}. You had been in range for {iters_in_range} "
                        )
                    success = False
                    break
                else:  # need to move to next range
                    if debug:
                        print(
                            f"At {i}, Range is currently "
                            + str(cur_range_i)
                            + ". Moving to next one"
                        )
                    update_range(current_range, iters_in_range, i, cur_range_i, rawdata_r_sm, ranges)
                    cur_range_i += 1
                    iters_in_range = 0

            # did not get enough at last range
            else:
                if debug:
                    print(
                        f"Incorrect slope was found at {str(i)}.  It was supposed to be \
                        {current_range.direction} but slope was {current_slope}. \
                        . Current Range was + {str(current_range)} Range i was "
                        + str(cur_range_i)
                    )
                success = False
                break

        if iters_in_range > current_range.upperN(N):
            if debug:
                print(f"At {i}, went over the allowed range for range {current_range} with {iters_in_range} iterations")
            success = False
            break

        i += 1
        iters_in_range += 1

        
    return success