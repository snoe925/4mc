/**
 4MC
 Copyright (c) 2018, Shannon Noe
 BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

 Redistribution and use in source and binary forms, with or without modification,
 are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this
 list of conditions and the following disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice, this
 list of conditions and the following disclaimer in the documentation and/or
 other materials provided with the distribution.

 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 You can contact 4MC author at :
 - 4MC source repository : https://github.com/carlomedas/4mc

 LZ4 - Copyright (C) 2011-2014, Yann Collet - BSD 2-Clause License.
 You can contact LZ4 lib author at :
 - LZ4 source repository : http://code.google.com/p/lz4/
 **/

/**
 * Hadoop Filesystem implementation for http or https access to 4mz or 4mc files.
 * Since 4mz files have internal checksums, data corruption can be detected.
 * To preserve the capability to seek the HTTP server must implement HEAD with
 * content-length and HTTP Range headers properly.
 *
 * This implementation is based on the JDK URLConnection class. The http and https URI
 * and protocol support are implemented by the JDK.
 *
 * // Either configure the core-site.xml for hadoop or
 * sc.hadoopConfiguration.set("fs.http.impl", "com.hadoop.compression.fourmc.fs.FileSystem")
 * val lines = sc.newAPIHadoopFile("http://~yourserver and port~/~your file~.4mz",
 *     classOf[com.hadoop.mapreduce.FourMzTextInputFormat],
 *     classOf[org.apache.hadoop.io.LongWritable],
 *     classOf[org.apache.hadoop.io.Text])
 *     .map(_._2.toString)
 *
 * In core-site.xml for hadoop
 * <property>
 *   <name>fs.http.impl</name>
 *   <value>com.hadoop.compression.fourmc.fs.FileSystem</value>
 * </property>
 * <property>
 *   <name>fs.https.impl</name>
 *   <value>com.hadoop.compression.fourmc.fs.FileSystem</value>
 * </property>
 */

package com.hadoop.compression.fourmc.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

class HttpRangeInputStream extends FSInputStream
{
    InputStream inputStream;
    long position = 0;
    final URL url;

    public HttpRangeInputStream( URL url ) throws IOException
    {
        this.url = url;
        inputStream = getInputStream(url, 0L);
    }

    private InputStream getInputStream( URL url, long offset ) throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod( "GET" );
        if (offset > 0) {
            connection.setRequestProperty("Range", "bytes=" + offset + "-");
        }
        int retry = 5;
        InputStream in = null;
        while (retry-- >= 0) {
            try {
                connection.connect();
                in = connection.getInputStream();
            } catch (IOException ex) {
                if (retry == 0) {
		    throw ex;
                }
            }
        }
        return in;
    }

    @Override
    public int read() throws IOException
    {
        byte[] b = new byte[1];
        int n = inputStream.read(b, 0, 1);
        if (n != 1) {
            throw new IOException("Failed to read http stream");
        }
        return b[0];
    }

    @Override
    public int read( byte[] b, int off, int len ) throws IOException
    {
        int n = inputStream.read( b, off, len );
        if (n > 0) {
            position += n;
        }
        return n;
    }

    @Override
    public void close() throws IOException
    {
        inputStream.close();
    }

    @Override
    public void seek( long pos ) throws IOException
    {
        if( getPos() == pos )
            return;

        inputStream.close();

        position = pos;
        inputStream = getInputStream(url, pos);
    }

    @Override
    public long getPos() throws IOException
    {
        if (inputStream == null) {
            throw new IOException("rhttps:// input stream is null.");
        }
        return position;
    }

    @Override
    public boolean seekToNewSource( long targetPos ) throws IOException
    {
        if (inputStream == null) {
            throw new IOException("rhttps:// input stream is null.");
        }
        return false;
    }
}

public class FileSystem extends org.apache.hadoop.fs.FileSystem
{
    public static final String HTTP_SCHEME = "http";
    public static final String HTTPS_SCHEME = "https";
    private static long MAX_BLOCK_4MC = 4 * 1024 * 1024;

    static
    {
        HttpURLConnection.setFollowRedirects( true );
    }

    /** Field scheme */
    private String scheme;
    /** Field authority */
    private String authority;

    @Override
    public void initialize(URI uri, Configuration configuration ) throws IOException
    {
        setConf( configuration );

        scheme = uri.getScheme();
        authority = uri.getAuthority();
    }

    @Override
    public URI getUri()
    {
        try
        {
            return new URI( scheme, authority, null, null, null );
        }
        catch( URISyntaxException exception )
        {
            throw new RuntimeException( "failed parsing uri", exception );
        }
    }

    @Override
    public FileStatus[] globStatus( Path path, PathFilter pathFilter ) throws IOException
    {
        FileStatus fileStatus = getFileStatus( path );

        if( fileStatus == null )
            return null;

        return new FileStatus[]{fileStatus};
    }

    @Override
    public FSDataInputStream open( Path path, int i ) throws IOException
    {
        URL url = makeUrl( path );

        return new FSDataInputStream( new HttpRangeInputStream( url ) );
    }

    @Override
    public boolean exists( Path path ) throws IOException
    {
        URL url = makeUrl( path );

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod( "HEAD" );
        connection.connect();

        return connection.getResponseCode() == 200;
    }

    @Override
    public FileStatus getFileStatus( Path path ) throws IOException
    {
        URL url = makeUrl( path );

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod( "HEAD" );
        connection.setUseCaches(false);
        connection.setDoInput(true);
        connection.setDoOutput(true);
        connection.connect();

        if( connection.getResponseCode() != 200 )
            throw new FileNotFoundException( "could not find file: " + path );

        connection.getContent();

        long length = connection.getContentLengthLong();
        if (length < 12) {
            throw new IOException("4mc/4mz file cannot be empty");
        }

        long modified = connection.getHeaderFieldDate( "Last-Modified", System.currentTimeMillis() );

        return new FileStatus( length, false, 1, MAX_BLOCK_4MC, modified, path );
    }

    @Override
    public Path getWorkingDirectory()
    {
        return new Path( "/" ).makeQualified( getUri(), new Path("/") );
    }

    @Override
    public void setWorkingDirectory( Path f )
    {
    }

    private URL makeUrl( Path path ) throws IOException
    {
        if( path.toString().startsWith( scheme ) )
            return URI.create( path.toString() ).toURL();

        try
        {
            return new URI( scheme, authority, path.toString(), null, null ).toURL();
        }
        catch( URISyntaxException exception )
        {
            throw new IOException( exception.getMessage() );
        }
    }

    @Override
    public FileStatus[] listStatus( Path path ) throws IOException
    {
        return new FileStatus[]{getFileStatus( path )};
    }

    /*
     * unimplemented methods because this is read-only access
     */

    private static final String NOT_SUPPORTED = " not supported on read-only filesystem rhttps://";

    @Override
    public FSDataOutputStream create( Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress ) throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException( "create " + NOT_SUPPORTED );
    }

    @Override
    public boolean rename( Path path, Path path1 ) throws IOException
    {
        throw new UnsupportedOperationException( "rename " + NOT_SUPPORTED );
    }

    @Deprecated
    @Override
    public boolean delete( Path path ) throws IOException
    {
        throw new UnsupportedOperationException( "delete" + NOT_SUPPORTED );
    }

    @Override
    public boolean delete( Path path, boolean b ) throws IOException
    {
        throw new UnsupportedOperationException( "delete" + NOT_SUPPORTED );
    }

    @Override
    public boolean mkdirs( Path path, FsPermission fsPermission ) throws IOException
    {
        throw new UnsupportedOperationException( "mkdirs" + NOT_SUPPORTED );
    }

    public FSDataOutputStream append( Path f, int bufferSize, Progressable progress ) throws IOException
    {
        throw new UnsupportedOperationException( "append" + NOT_SUPPORTED );
    }
}
