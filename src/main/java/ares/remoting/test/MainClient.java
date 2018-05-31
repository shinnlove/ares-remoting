package ares.remoting.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 客户端调用远程服务。
 *
 * @author liyebing created on 16/10/5.
 * @version $Id$
 */
public class MainClient {

    private static final Logger logger = LoggerFactory.getLogger(MainClient.class);

    public static void main(String[] args) throws Exception {

        //引入远程服务
        // 使用`ares-client.xml`加载spring上下文，其中定义了一个`bean id = "remoteHelloService"`
        final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("ares-client.xml");
        //获取远程服务
        // 从spring上下文中根据id取出这个bean
        final HelloService helloService = (HelloService) context.getBean("remoteHelloService");

        long count = 1000000000000000000L;

        //调用服务并打印结果
        for (int i = 0; i < count; i++) {
            try {
                String result = helloService.sayHello("liyebing,i=" + i);
                System.out.println(result);
            } catch (Exception e) {
                logger.warn("--------", e);
            }
        }

        //关闭jvm
        System.exit(0);
    }
}
