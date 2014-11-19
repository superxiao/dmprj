import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class processing implements Runnable{
	public void run() {
			FindingFrequentItemsets f = new FindingFrequentItemsets();
			System.out.println("Server start processing!");
			try {
				ServerSocket ss = new ServerSocket(8001);
				while (true){
					Socket s = ss.accept();
					DataInputStream dis = new DataInputStream(s.getInputStream());
					String tmp = new String(dis.readUTF());
					System.out.println(tmp);
					if (tmp.compareTo("clean")==0) FileProcessing.clean();
					else {
						String [] arguments = tmp.split(" ");
						FileProcessing.upload(arguments[0]);
						List <String>l = new ArrayList<String>();
						l.add("input");
						l.add(arguments[1]);
						l.add(arguments[2]);
						f.run((String [])l.toArray(new String[0]));
						}
			}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
}
public class Main{
	public static GUI m = new GUI();
		
	public static void main(String[] args) {
    	processing p = new processing();
    	Thread t = new Thread(p);
    	t.start();
		m.init();
    }
}
