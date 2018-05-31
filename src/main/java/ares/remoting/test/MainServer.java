package ares.remoting.test;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 服务端发布远程服务。
 *
 * @author liyebing created on 16/10/5.
 * @version $Id$
 */
public class MainServer {


    public static void main(String[] args) throws Exception {

        //发布服务
        final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("ares-server.xml");
        System.out.println(" 服务发布完成");
    }
}
