import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import javax.swing.*;
import javax.swing.text.JTextComponent;

class GUIPrintStream extends PrintStream{
    private JTextArea component;
    private StringBuffer sb = new StringBuffer();
    public GUIPrintStream(OutputStream out, JTextArea component){
        super(out);
        this.component = component;
    }
    private void updateTextArea(final String text) {
    	  SwingUtilities.invokeLater(new Runnable() {
    	    public void run() {
    	    	component.append(text);
    	    }
    	  });
    	}
    public void write(int b) {
        updateTextArea(String.valueOf((char) b));
      }
    public void write(byte[] b, int off, int len) {
        updateTextArea(new String(b, off, len));
      }
    public void write(byte[] b)  {
        write(b, 0, b.length);
      }
}
public class GUI {
		static int DEFAULT_WIDTH = 800;
		static int DEFAULT_HEITHT = 500;
		static Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	    
		static JMenu start, settings, help;
		static JMenuItem startprocessing, checkresult, clean, exit, setargs, about;
		static JPanel panel ;
		static JButton button;
		static JTextArea textarea;
		static String[] argumentsStrings = null;
		static String arugment = null;
		//public static boolean runmapreduce = false;
		public static boolean finished = false;
	public  void init(){
		JFrame frame = new JFrame();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame. setTitle("Find frequent item set");
        frame.setBounds((int)(screenSize.width-DEFAULT_WIDTH)/2,  (int)(screenSize.height-DEFAULT_HEITHT)/2, DEFAULT_WIDTH, DEFAULT_HEITHT);
        frame.setResizable(false);
        JMenuBar menubar = new JMenuBar();
        start = new JMenu("Start");
		 settings = new JMenu("Settings");
		 help = new JMenu("Help");
		 menubar.add(start);
		 menubar.add(settings);
		 menubar.add(help);
		 startprocessing = new JMenuItem("Strat Processing");
		 startprocessing.addActionListener(new ActionListener() {
	            public void actionPerformed(java.awt.event.ActionEvent evt) {
	            	try {
	            		String localHost="127.0.0.1"; 
	            		//System.out.println("Send info "+localHost);
						Socket s = new Socket(localHost, 8001);
						DataOutputStream dos = new DataOutputStream(s.getOutputStream());
						dos.writeUTF(arugment);
						dos.flush();
						dos.close();
						s.close();
						//System.out.println("Send argument:"+arugment+" finished!");;
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
	                System.out.println("Start mapreduce!");
	            }
	        });
		 checkresult = new JMenuItem("Check the Result");
		 checkresult.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				try {
					String finalfilelocation = FileProcessing.getLocation("result");
					if (finalfilelocation != null){
						List<String> lines = FileProcessing.readFile(finalfilelocation);
						if (lines != null){
							int n = lines.size();
							//System.out.println("\n");
							for (int i = 0; i < n; i++) System.out.println(lines.get(i));
							System.out.println("All done.\n");
						}
					}else{
						System.out.println("No result.");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		 clean = new JMenuItem("Clean HDFS");
		 clean.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				try {
            		String localHost="127.0.0.1"; 
					Socket s = new Socket(localHost, 8001);
					DataOutputStream dos = new DataOutputStream(s.getOutputStream());
					dos.writeUTF("clean");
					dos.flush();
					dos.close();
					s.close();
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
		});
		 exit = new JMenuItem("Exit");
		 exit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				System.exit(0);
			}
		});
		 start.add(startprocessing);
		 start.add(checkresult);
		 start.add(clean);
		 start.addSeparator();
		 start.add(exit);
		 setargs = new JMenuItem("Input Settings");
		 setargs.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				String inputValue = JOptionPane.showInputDialog("Please input the arguments");
				if (inputValue != null) {
					argumentsStrings = inputValue.split(" ");
					if (argumentsStrings.length != 3){
					JOptionPane.showMessageDialog(null, "Invalid arguments", "Fail", 	JOptionPane.ERROR_MESSAGE);
					}else{
						arugment = inputValue;
					}
				}
				else JOptionPane.showMessageDialog(null, "No input", "Fail", 	JOptionPane.ERROR_MESSAGE);
			}
		});
		 settings.add(setargs);
		 about = new JMenuItem("About");
		 about.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				JOptionPane.showMessageDialog(null, "Team Member: Zixuan Wang & Jian Xiao", "Team", 	JOptionPane.INFORMATION_MESSAGE);
			}
		});
		 help.add(about);
        frame.setJMenuBar(menubar);
        frame.setLayout(new FlowLayout());
        textarea = new JTextArea(); 
        textarea.setEditable(false);	
        textarea.setRows(25);
        textarea.setColumns(60);
        textarea.setAutoscrolls(true);
        textarea.setLineWrap(true);
        JScrollPane pane = new JScrollPane(textarea);
        frame.add(pane);
        
        button = new JButton();
        button.setText("Clean Result");
        button.setSize(20, 10);
        button.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                textarea.setText("");
            }
        });
        frame.add(button);
        frame.setVisible(true);
        System.setOut(new GUIPrintStream(System.out, textarea));
    	System.setErr(new GUIPrintStream(System.out, textarea));
	}
}