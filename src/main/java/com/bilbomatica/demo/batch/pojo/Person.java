package com.bilbomatica.demo.batch.pojo;

import org.mongodb.morphia.annotations.Entity;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="Person")
@XmlAccessorType(XmlAccessType.FIELD)
@Entity(value = "persons", noClassnameStored = true)
public class Person extends BaseEntity {
    private static final long serialVersionUID = 1L;

    @Min(value = 1, message = "The id must be valid")
    @XmlElement(name="selfId")
    private int selfId;

    @Min(value = 1, message = "The id must be valid")
    @XmlElement(name="familyId")
    private int familyId;

    @NotNull(message = "Name cannot be null")
    @Size(min = 5, max = 150, message= "Name must be between 5 and 150 characters")
    @XmlElement(name="nombre")
    private String nombre;

    public Person() {

//        this.selfId = 0 ;
//        this.familyId = 0;
//        this.nombre = "";

    }

    public int getFamilyId() {
        return familyId;
    }

    public void setFamilyId(int familyId) {
        this.familyId = familyId;
    }

    public int getselfId() {
        return selfId;
    }

    public void setSelfId(int personId) {
        this.selfId = personId;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    @Override
    public String toString() {
        return "Person{" +
                "selfId=" + selfId +
                ", familyId=" + familyId +
                ", nombre='" + nombre + '\'' +
                ", _id=" + _id +
                '}';
    }
}
