package com.youzhu.pre12;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SumCount {

    /*
    integer* double 容易空指针
    int 默认值为0
     */
    private int vcSum;
    private int count;
}
