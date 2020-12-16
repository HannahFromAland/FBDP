package join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class InfoBean implements Writable {
    private int user_id;
    private String log;

    private String info;
    private String flag;
    public InfoBean(){}

    public void set(int user_id,String log,String info,String flag){
        this.user_id = user_id;
        this.log = log;

        this.info = info;
        this.flag = flag;
    }

    public int getUser_id(){
        return user_id;
    }
    public void setUser_id(int user_id){
        this.user_id = user_id;
    }

    public String getLog(){
        return log;
    }
    public void setLog(String log){
        this.log = log;
    }

    public String getInfo(){
        return info;
    }
    public void setInfo(String info){
        this.info = info;
    }

    public String getFlag(){
        return flag;
    }
    public void setFlag(){
        this.flag = flag;
    }

    @Override
    public void readFields(DataInput in)throws IOException{
        this.user_id = in.readInt();
        this.log = in.readUTF();
        this.info = in.readUTF();
        this.flag =  in.readUTF();
    }
    @Override
    public void write(DataOutput out)throws  IOException{
        out.writeInt(user_id);
        out.writeUTF(log);

        out.writeUTF(info);
        out.writeUTF(flag);
    }
    public String toLog(){
        return user_id+" "+log;
    }
    public String toInfo(){
        return info;
    }
    @Override
    public String toString(){
        return user_id+" "+log+" "+info+" "+flag;
    }
}
