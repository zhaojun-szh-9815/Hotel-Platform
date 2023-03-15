package cn.itcast.hotel.mq;

import cn.itcast.hotel.constants.MQConstants;
import cn.itcast.hotel.service.impl.HotelService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HotelListener {
    @Autowired
    private HotelService hotelService;

    @RabbitListener(queues = MQConstants.HOTEL_INSERT_QUEUE)
    public void listenHotelInsertOrUpdate(Long id) {
        hotelService.insertById(id);
    }

    @RabbitListener(queues = MQConstants.HOTEL_DELETE_QUEUE)
    public void listenHotelDelete(Long id) {
        hotelService.deleteById(id);
    }
}
