package dev.daodao.data.zipper.demo;

import lombok.extern.slf4j.Slf4j;
import dev.daodao.data.zipper.core.task.ZipperTask;

@Slf4j
public class ZipperDemo {

    public static void main(String[] args) throws Exception {

        Class<?> clazz = ZipperDemo.class;

        ZipperTask.run(clazz, args);

    }

}
