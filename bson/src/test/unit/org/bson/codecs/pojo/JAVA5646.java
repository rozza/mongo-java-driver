package org.bson.codecs.pojo;


import java.util.List;
import org.junit.jupiter.api.Test;

class Counter<T extends Number> {
    public T count;
}

class Person<T extends Counter<?>> {
    public T counter;
}

class Pojo1 {
    public List<? extends Person<? extends Counter<Integer>>> persons;
}


public class JAVA5646 {

    @Test
    void testBuilder() {

        ClassModelBuilder<Pojo1> builder = ClassModel.builder(Pojo1.class);// throws
        ClassModel<Pojo1> pojo1 = builder.build();
        pojo1.getPropertyModels().forEach(p -> System.out.println(p.getTypeData()));

//        ClassModel.builder(Pojo2.class); // throws
//        ClassModel.builder(Pojo3.class); // ok
//        ClassModel.builder(Pojo4.class); // ok
//        ClassModel.builder(Pojo5.class); // ok
//        ClassModel.builder(Pojo6.class); // throws

    }
}
