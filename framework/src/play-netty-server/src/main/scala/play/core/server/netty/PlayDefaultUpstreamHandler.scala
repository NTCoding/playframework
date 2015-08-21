/*
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package play.core.server.netty

import akka.stream.Materializer
import akka.util.ByteString
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpHeaders._
import org.jboss.netty.handler.codec.http.websocketx._
import org.jboss.netty.handler.codec.frame.TooLongFrameException
import org.jboss.netty.handler.ssl._

import org.jboss.netty.channel.group._
import org.reactivestreams.{ Subscription, Subscriber, Publisher }
import play.api._
import play.api.http.websocket._
import play.api.http.{ HttpErrorHandler, DefaultHttpErrorHandler }
import play.api.libs.streams.{ Streams, Accumulator }
import play.api.mvc._
import play.api.libs.iteratee._
import play.api.libs.iteratee.Input._
import play.core.server.NettyServer
import play.core.server.common.{ WebSocketFlowHandler, ForwardedHeaderHandler, ServerRequestUtils, ServerResultUtils }
import play.core.system.RequestIdProvider
import scala.collection.JavaConverters._
import scala.util.control.{ NonFatal, Exception }
import com.typesafe.netty.http.pipelining.{ OrderedDownstreamChannelEvent, OrderedUpstreamMessageEvent }
import scala.concurrent.Future
import java.net.{ InetSocketAddress, URI }
import java.io.IOException

private[play] class PlayDefaultUpstreamHandler(server: NettyServer, allChannels: DefaultChannelGroup) extends SimpleChannelUpstreamHandler with WebSocketHandler with RequestBodyHandler {

  import PlayDefaultUpstreamHandler._

  private lazy val forwardedHeaderHandler = new ForwardedHeaderHandler(
    ForwardedHeaderHandler.ForwardedHeaderHandlerConfig(server.applicationProvider.get.toOption.map(_.configuration)))

  private lazy val nettyHttpRequestHandler = new nettyHttpRequestHandler(forwardedHeaderHandler, server)

  /**
   * Sends a simple response with no body, then closes the connection.
   */
  private def sendSimpleErrorResponse(ctx: ChannelHandlerContext, status: HttpResponseStatus): Unit = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status)
    response.headers().set(Names.CONNECTION, "close")
    response.headers().set(Names.CONTENT_LENGTH, "0")
    ctx.getChannel.write(response).addListener(ChannelFutureListener.CLOSE)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent): Unit = {

    event.getCause match {
      // IO exceptions happen all the time, it usually just means that the client has closed the connection before fully
      // sending/receiving the response.
      case e: IOException =>
        logger.trace("Benign IO exception caught in Netty", e)
        event.getChannel.close()
      case e: TooLongFrameException =>
        logger.warn("Handling TooLongFrameException", e)
        sendSimpleErrorResponse(ctx, HttpResponseStatus.REQUEST_URI_TOO_LONG)
      case e: IllegalArgumentException if Option(e.getMessage).exists(_.contains("Header value contains a prohibited character")) =>
        // https://github.com/netty/netty/blob/netty-3.9.3.Final/src/main/java/org/jboss/netty/handler/codec/http/HttpHeaders.java#L1075-L1080
        logger.debug("Handling Header value error", e)
        sendSimpleErrorResponse(ctx, HttpResponseStatus.BAD_REQUEST)
      case e =>
        logger.error("Exception caught in Netty", e)
        event.getChannel.close()
    }

  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    Option(ctx.getPipeline.get(classOf[SslHandler])).map { sslHandler =>
      sslHandler.handshake()
    }
  }

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    allChannels.add(e.getChannel)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {

      case nettyHttpRequest: HttpRequest => nettyHttpRequestHandler.handle(nettyHttpRequest, ctx)

      case unexpected => logger.error("Oops, unexpected message received in NettyServer (please report this problem): " + unexpected)

    }
  }

  def socketOut(ctx: ChannelHandlerContext): Iteratee[Message, Unit] = {
    import play.api.libs.iteratee.Execution.Implicits.trampoline

    val channel = ctx.getChannel
    import NettyFuture._

    def iteratee: Iteratee[Message, _] = Cont {
      case El(message) =>
        val nettyFrame: WebSocketFrame = message match {
          case TextMessage(text) => new TextWebSocketFrame(text)
          case BinaryMessage(bytes) => new BinaryWebSocketFrame(ChannelBuffers.wrappedBuffer(bytes.asByteBuffer))
          case PingMessage(data) => new PingWebSocketFrame(ChannelBuffers.wrappedBuffer(data.asByteBuffer))
          case PongMessage(data) => new PongWebSocketFrame(ChannelBuffers.wrappedBuffer(data.asByteBuffer))
          case CloseMessage(status, reason) => new CloseWebSocketFrame(status.getOrElse(1000), reason)
        }
        Iteratee.flatten(channel.write(nettyFrame).toScala.map(_ => iteratee))
      case EOF => Done(())
      case Empty => iteratee
    }

    iteratee.mapM { _ =>
      channel.close().toScala
    }.map(_ => ())
  }

  def sendDownstream(subSequence: Int, last: Boolean, message: Object)(implicit ctx: ChannelHandlerContext, oue: OrderedUpstreamMessageEvent) = {
    val ode = new OrderedDownstreamChannelEvent(oue, subSequence, last, message)
    ctx.sendDownstream(ode)
    ode.getFuture
  }
}

object PlayDefaultUpstreamHandler {
  private val logger = Logger(classOf[PlayDefaultUpstreamHandler])
}
