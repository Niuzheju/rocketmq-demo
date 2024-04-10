package com.example.rocketmq.bean;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * @Author niuzheju
 * @Date 14:09 2024/4/9
 */

@Getter
@Setter
public class OrderBean {

    private Long id;
    private String orderNo;
    private Long count;
    private BigDecimal price;
    private LocalDateTime orderTime;
    private String desc;


}
