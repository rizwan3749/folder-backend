const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");
const http = require("http");
const socketIo = require("socket.io");
require("dotenv").config({ path: "./config.env" });
const path = require("path");
const jwt = require("jsonwebtoken");
const cron = require("node-cron");
const moment = require("moment-timezone");
const ntpClient = require("ntp-client");
const User = require("./models/User");
const Call = require("./models/Call");
const GroupCall = require("./models/GroupCall");
const TimeSettings = require("./models/TimeSettings");
const ScheduledDisable = require("./models/ScheduledDisable");

const app = express();
const server = http.createServer(app);

// Increase server timeout for large file uploads (5 minutes)
server.timeout = 5 * 60 * 1000; // 5 minutes
server.keepAliveTimeout = 5 * 60 * 1000; // 5 minutes
server.headersTimeout = 5 * 60 * 1000; // 5 minutes

// Socket.io setup with increased buffer size for large files
const io = socketIo(server, {
  cors: {
    origin: process.env.FRONTEND_URL || "http://localhost:3000",
    methods: ["GET", "POST"],
  },
  maxHttpBufferSize: 100 * 1024 * 1024, // 100MB for large file support
  pingTimeout: 60000, // 60 seconds
  pingInterval: 25000, // 25 seconds
});

// Make io and globalTimeOffset available to routes
app.set("io", io);
app.use((req, res, next) => {
  req.app.set("globalTimeOffset", globalTimeOffset);
  next();
});

// Middleware
app.use(cors());
app.use(express.json({ limit: "100mb" })); // Increased to 100MB
app.use(express.urlencoded({ limit: "100mb", extended: true })); // Increased to 100MB

// Serve static files for profile images
app.use("/uploads", express.static("uploads"));

// Serve static files for push notification demo
app.use(express.static(path.join(__dirname, "public")));

// Serve static files from frontend build
app.use(express.static(path.join(__dirname, "dist")));

// Routes
app.use("/api/auth", require("./routes/auth"));
app.use("/api/users", require("./routes/users"));
app.use("/api/groups", require("./routes/groups"));
app.use("/api/messages", require("./routes/messages"));
app.use("/api/calls", require("./routes/calls"));
app.use("/api/group-calls", require("./routes/groupCalls"));
app.use("/api/time-settings", require("./routes/timeSettings"));
app.use("/api/scheduled-disable", require("./routes/scheduledDisable"));
app.use("/api/push-notifications", require("./routes/pushNotifications"));
app.use("/api/fcm-notifications", require("./routes/fcmNotifications"));
app.use("/api/daily-updates", require("./routes/dailyUpdates"));
app.use("/api/tasks", require("./routes/tasks"));

// Import push notification helpers
const { sendPushToUser } = require("./routes/pushNotifications");
const { sendFCMToUser } = require("./services/fcmService");

// Authenticate socket connections with JWT and track active users
const activeUsers = new Map();

io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth && socket.handshake.auth.token;
    if (!token) {
      return next(new Error("Authentication error: No token provided"));
    }
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    const user = await User.findById(decoded.userId).select("-password");
    if (!user) {
      return next(new Error("Authentication error: User not found"));
    }
    socket.userId = user._id.toString();
    socket.user = user;
    next();
  } catch (err) {
    next(new Error("Authentication error: Invalid token"));
  }
});

io.on("connection", (socket) => {
  console.log(
    `✅ User connected: ${socket.user?.name || socket.userId} - ${socket.id}`
  );

  // Track active users and join personal room
  activeUsers.set(socket.userId, {
    socketId: socket.id,
    user: socket.user,
    lastSeen: new Date(),
  });
  socket.join(socket.userId);

  // Update user lastSeen in database to now (online)
  User.findByIdAndUpdate(socket.userId, {
    lastSeen: new Date(),
  }).catch((err) => console.error("Error updating lastSeen:", err));

  // Broadcast to all connected users that this user is now online
  socket.broadcast.emit("user-online", {
    userId: socket.userId,
    user: {
      _id: socket.userId,
      name: socket.user?.name,
      email: socket.user?.email,
      profileImage: socket.user?.profileImage,
    },
    lastSeen: new Date(),
  });

  // Send list of all currently online users to the newly connected user
  const onlineUsersList = Array.from(activeUsers.entries()).map(
    ([userId, data]) => ({
      userId,
      user: {
        _id: userId,
        name: data.user?.name,
        email: data.user?.email,
        profileImage: data.user?.profileImage,
      },
      lastSeen: data.lastSeen,
    })
  );
  socket.emit("online-users", onlineUsersList);

  // Handle personal messages
  socket.on("send-message", async (data) => {
    try {
      console.log("🔄 Backend received send-message:", data);
      const {
        receiver,
        message,
        sender,
        messageType,
        fileUrl,
        fileName,
        fileSize,
        fileType,
        replyTo,
      } = data;

      // IMPORTANT: Save message to database first
      const Message = require("./models/Message");
      const messageData = {
        sender: sender,
        receiver: receiver,
        message: message || "",
        messageType: messageType || "text",
      };

      // Add replyTo if present (extract messageId from replyTo object)
      if (replyTo && replyTo.messageId) {
        messageData.replyTo = replyTo.messageId;
      }

      // Add file data if present
      if (fileUrl) {
        messageData.fileUrl = fileUrl;
        messageData.fileName = fileName;
        messageData.fileSize = fileSize;
        messageData.fileType = fileType;
      }

      const newMessage = new Message(messageData);
      await newMessage.save();
      console.log("💾 Message saved to database:", newMessage._id);

      // Populate message data for response (including replyTo)
      await newMessage.populate("sender", "name email profileImage");
      await newMessage.populate("receiver", "name email profileImage");
      if (newMessage.replyTo) {
        await newMessage.populate({
          path: "replyTo",
          select: "message messageType fileUrl fileName sender createdAt",
          populate: { path: "sender", select: "name email profileImage" }
        });
      }

      // Prepare replyTo data for socket emission
      let replyToData = null;
      if (newMessage.replyTo && typeof newMessage.replyTo === 'object') {
        replyToData = {
          messageId: newMessage.replyTo._id,
          message: newMessage.replyTo.message || newMessage.replyTo.fileName || 'Media',
          sender: typeof newMessage.replyTo.sender === 'object' 
            ? newMessage.replyTo.sender.name 
            : newMessage.replyTo.sender,
          messageType: newMessage.replyTo.messageType || 'text',
          fileUrl: newMessage.replyTo.fileUrl || null,
          fileName: newMessage.replyTo.fileName || null
        };
      }

      // Emit to receiver if they're online
      const receiverMessage = {
        id: newMessage._id, // Use actual message ID from database
        sender: newMessage.sender,
        receiver: receiver,
        message: newMessage.message,
        messageType: newMessage.messageType,
        fileUrl: newMessage.fileUrl,
        fileName: newMessage.fileName,
        fileSize: newMessage.fileSize,
        fileType: newMessage.fileType,
        timestamp: newMessage.createdAt,
        createdAt: newMessage.createdAt,
        isFromOtherUser: true,
        _id: newMessage._id,
        replyTo: replyToData,
      };
      console.log(
        "📤 Emitting to receiver:",
        receiver,
        "message:",
        receiverMessage
      );
      socket.to(receiver).emit("receive-message", receiverMessage);

      // Send push notification if receiver is not online
      const receiverOnline = activeUsers.has(receiver);
      if (!receiverOnline) {
        const senderName = newMessage.sender?.name || "Someone";
        const messagePreview =
          newMessage.messageType === "text"
            ? newMessage.message.substring(0, 100)
            : `Sent a ${newMessage.messageType}`;

        // Try FCM first, fallback to Web Push
        sendFCMToUser(
          receiver,
          `New message from ${senderName}`,
          messagePreview,
          {
            type: "personal-message",
            senderId: sender,
            senderName: senderName,
            receiverId: receiver,
            messageId: newMessage._id.toString(),
            icon: newMessage.sender?.profileImage || "/icon.png",
          }
        )
          .then((result) => {
            if (!result.success) {
              return sendPushToUser(
                receiver,
                `New message from ${senderName}`,
                messagePreview,
                newMessage.sender?.profileImage || "/icon.png",
                {
                  type: "personal-message",
                  senderId: sender,
                  senderName: senderName,
                  receiverId: receiver,
                  messageId: newMessage._id.toString(),
                }
              );
            }
          })
          .catch((err) => {
            console.log("Push notification failed:", err.message);
          });
      }

      // Emit back to sender for confirmation
      const confirmationMessage = {
        id: newMessage._id,
        sender: newMessage.sender,
        receiver: receiver,
        message: newMessage.message,
        messageType: newMessage.messageType,
        fileUrl: newMessage.fileUrl,
        fileName: newMessage.fileName,
        fileSize: newMessage.fileSize,
        fileType: newMessage.fileType,
        timestamp: newMessage.createdAt,
        createdAt: newMessage.createdAt,
        isConfirmMessage: true,
        _id: newMessage._id,
        replyTo: replyToData,
      };
      console.log(
        "📤 Emitting confirmation back to sender:",
        confirmationMessage
      );
      socket.emit("message-sent", confirmationMessage);
      console.log("✅ Message saved and handling completed successfully");
    } catch (error) {
      console.error("❌ Error in backend message handling:", error);
      socket.emit("error", { message: "Failed to send message" });
    }
  });

  // Handle group messages
  socket.on("send-group-message", async (data) => {
    try {
      const {
        groupId,
        message,
        sender,
        messageType,
        fileUrl,
        fileName,
        fileSize,
        fileType,
        replyTo,
      } = data;

      // Save group message to database
      const Message = require("./models/Message");
      const Group = require("./models/Group");

      const messageData = {
        sender: sender,
        group: groupId,
        message: message || "",
        messageType: messageType || "text",
      };

      // Add replyTo if present (extract messageId from replyTo object)
      if (replyTo && replyTo.messageId) {
        messageData.replyTo = replyTo.messageId;
      }

      // Add file data if present
      if (fileUrl) {
        messageData.fileUrl = fileUrl;
        messageData.fileName = fileName;
        messageData.fileSize = fileSize;
        messageData.fileType = fileType;
      }

      const newMessage = new Message(messageData);
      await newMessage.save();
      console.log("💾 Group message saved to database:", newMessage._id);

      // Populate message data (including replyTo)
      await newMessage.populate("sender", "name email profileImage");
      await newMessage.populate("group", "name");
      if (newMessage.replyTo) {
        await newMessage.populate({
          path: "replyTo",
          select: "message messageType fileUrl fileName sender createdAt",
          populate: { path: "sender", select: "name email profileImage" }
        });
      }

      // Prepare replyTo data for socket emission
      let replyToData = null;
      if (newMessage.replyTo && typeof newMessage.replyTo === 'object') {
        replyToData = {
          messageId: newMessage.replyTo._id,
          message: newMessage.replyTo.message || newMessage.replyTo.fileName || 'Media',
          sender: typeof newMessage.replyTo.sender === 'object' 
            ? newMessage.replyTo.sender.name 
            : newMessage.replyTo.sender,
          messageType: newMessage.replyTo.messageType || 'text',
          fileUrl: newMessage.replyTo.fileUrl || null,
          fileName: newMessage.replyTo.fileName || null
        };
      }

      // Emit to all members of the group
      socket.to(groupId).emit("receive-group-message", {
        id: newMessage._id,
        groupId,
        sender: newMessage.sender,
        message: newMessage.message,
        messageType: newMessage.messageType,
        fileUrl: newMessage.fileUrl,
        fileName: newMessage.fileName,
        fileSize: newMessage.fileSize,
        fileType: newMessage.fileType,
        timestamp: newMessage.createdAt,
        createdAt: newMessage.createdAt,
        _id: newMessage._id,
        replyTo: replyToData,
      });

      // Send push notifications to offline group members
      const group = await Group.findById(groupId);
      if (group && group.members) {
        const senderName = newMessage.sender?.name || "Someone";
        const groupName = group.name || "Group";
        const messagePreview =
          newMessage.messageType === "text"
            ? newMessage.message.substring(0, 100)
            : `Sent a ${newMessage.messageType}`;

        group.members.forEach((member) => {
          const memberId = member.user.toString();
          // Skip sender and online members
          if (memberId !== sender && !activeUsers.has(memberId)) {
            // Try FCM first, fallback to Web Push
            sendFCMToUser(
              memberId,
              `${senderName} in ${groupName}`,
              messagePreview,
              {
                type: "group-message",
                groupId: groupId,
                groupName: groupName,
                senderId: sender,
                senderName: senderName,
                receiverId: memberId,
                messageId: newMessage._id.toString(),
                icon: newMessage.sender?.profileImage || "/icon.png",
              }
            )
              .then((result) => {
                if (!result.success) {
                  return sendPushToUser(
                    memberId,
                    `${senderName} in ${groupName}`,
                    messagePreview,
                    newMessage.sender?.profileImage || "/icon.png",
                    {
                      type: "group-message",
                      groupId: groupId,
                      groupName: groupName,
                      senderId: sender,
                      senderName: senderName,
                      receiverId: memberId,
                      messageId: newMessage._id.toString(),
                    }
                  );
                }
              })
              .catch((err) => {
                console.log("Push notification failed:", err.message);
              });
          }
        });
      }

      // Emit back to sender for confirmation
      socket.emit("group-message-sent", {
        id: newMessage._id,
        groupId,
        sender: newMessage.sender,
        message: newMessage.message,
        messageType: newMessage.messageType,
        fileUrl: newMessage.fileUrl,
        fileName: newMessage.fileName,
        fileSize: newMessage.fileSize,
        fileType: newMessage.fileType,
        timestamp: newMessage.createdAt,
        createdAt: newMessage.createdAt,
        _id: newMessage._id,
        replyTo: replyToData,
      });
      console.log("✅ Group message saved and handling completed successfully");
    } catch (error) {
      console.error("❌ Error in group message handling:", error);
      socket.emit("error", { message: "Failed to send group message" });
    }
  });

  // Join group room
  socket.on("join-group", (groupId) => {
    socket.join(groupId);
    console.log(`User joined group: ${groupId}`);
  });

  // Leave group room
  socket.on("leave-group", (groupId) => {
    socket.leave(groupId);
    console.log(`User left group: ${groupId}`);
  });

  // Handle typing indicators
  socket.on("typing", (data) => {
    socket.to(data.receiver || data.groupId).emit("user-typing", {
      sender: data.sender,
      isTyping: data.isTyping,
    });
  });

  // ========================
  // Call signaling handlers
  // ========================
  socket.on("call-initiate", async (data) => {
    try {
      const { receiverId, callType = "voice", callId } = data || {};
      console.log(`📞 Call initiate request:`, {
        receiverId,
        callType,
        callId,
        caller: socket.userId,
        callerName: socket.user?.name,
      });

      if (!receiverId) {
        socket.emit("call-error", { error: "Receiver ID is required" });
        return;
      }

      // Notify caller (confirmation)
      socket.emit("call-initiated", {
        callId,
        callType,
        receiver: await User.findById(receiverId).select("name avatar email"),
      });

      // Notify receiver if online - Check both string and object ID formats
      const receiverSocketData =
        activeUsers.get(receiverId) || activeUsers.get(receiverId.toString());

      console.log(`🔍 Looking for receiver ${receiverId}:`, {
        found: !!receiverSocketData,
        activeUsersCount: activeUsers.size,
        activeUserIds: Array.from(activeUsers.keys()),
      });

      if (receiverSocketData) {
        console.log(
          `✅ Receiver ${receiverId} is online, sending incoming-call to socket ${receiverSocketData.socketId}`
        );
        io.to(receiverSocketData.socketId).emit("incoming-call", {
          callId,
          caller: socket.user,
          callType,
        });
        console.log(
          `📤 Incoming call notification sent to receiver ${receiverId}`
        );
      } else {
        console.log(
          `❌ Receiver ${receiverId} is not online - sending push notification`
        );

        // Send push notification for incoming call
        const callerName = socket.user?.name || "Someone";
        const callTypeText = callType === "video" ? "Video" : "Voice";

        // Try FCM first, fallback to Web Push
        sendFCMToUser(
          receiverId,
          `📞 Incoming ${callTypeText} Call`,
          `${callerName} is calling you...`,
          {
            type: "incoming-call",
            callType: callType,
            callId: callId,
            callerId: socket.userId,
            callerName: callerName,
            callerImage: socket.user?.profileImage || "/icon.png",
            // Add fields that frontend expects
            senderId: socket.userId,
            senderName: callerName,
            senderImage: socket.user?.profileImage || "/icon.png",
            icon: socket.user?.profileImage || "/icon.png",
          }
        )
          .then((result) => {
            if (!result.success) {
              return sendPushToUser(
                receiverId,
                `📞 Incoming ${callTypeText} Call`,
                `${callerName} is calling you...`,
                socket.user?.profileImage || "/icon.png",
                {
                  type: "incoming-call",
                  callType: callType,
                  callId: callId,
                  callerId: socket.userId,
                  callerName: callerName,
                  // Add fields that frontend expects
                  senderId: socket.userId,
                  senderName: callerName,
                  senderImage: socket.user?.profileImage || "/icon.png",
                }
              );
            }
          })
          .catch((err) => {
            console.log("Call push notification failed:", err.message);
          });

        // Still notify the caller that receiver is offline
        socket.emit("call-error", {
          error: "Receiver is not online - notification sent",
          receiverId: receiverId,
          notificationSent: true,
        });
      }
    } catch (error) {
      console.error("Call initiate error:", error);
      socket.emit("call-error", { error: "Failed to initiate call" });
    }
  });

  socket.on("call-answer", async (data) => {
    try {
      const { callId, answer } = data || {};
      if (!callId) {
        socket.emit("call-error", { error: "Call ID is required" });
        return;
      }
      const call = await Call.findById(callId);
      if (!call) {
        socket.emit("call-error", { error: "Call not found" });
        return;
      }
      if (call.receiver.toString() !== socket.userId) {
        socket.emit("call-error", {
          error: "Not authorized to answer this call",
        });
        return;
      }
      call.status = "answered";
      if (answer) {
        call.answer = JSON.stringify(answer);
      }
      await call.save();
      const callerActive = activeUsers.get(call.caller.toString());
      if (callerActive) {
        io.to(callerActive.socketId).emit("call-answered", {
          callId: call._id,
          status: call.status,
          answer,
          receiver: socket.user,
          callType: call.callType,
        });
      }
    } catch (error) {
      console.error("Call answer error:", error);
      socket.emit("call-error", { error: "Failed to answer call" });
    }
  });

  socket.on("call-decline", async (data) => {
    try {
      const { callId } = data || {};
      const call = await Call.findById(callId);
      if (!call) {
        socket.emit("call-error", { error: "Call not found" });
        return;
      }
      if (call.receiver.toString() !== socket.userId) {
        socket.emit("call-error", {
          error: "Not authorized to decline this call",
        });
        return;
      }
      await call.markAsDeclined();
      socket.emit("call-declined", {
        callId: call._id,
        status: call.status,
        callType: call.callType,
      });
      const callerActive = activeUsers.get(call.caller.toString());
      if (callerActive) {
        io.to(callerActive.socketId).emit("call-declined", {
          callId: call._id,
          status: call.status,
          receiver: socket.user,
          callType: call.callType,
        });
      }
    } catch (error) {
      console.error("Call decline error:", error);
      socket.emit("call-error", { error: "Failed to decline call" });
    }
  });

  socket.on("call-end", async (data) => {
    try {
      const { callId } = data || {};
      const call = await Call.findById(callId);
      if (!call) {
        socket.emit("call-error", { error: "Call not found" });
        return;
      }
      if (
        call.caller.toString() !== socket.userId &&
        call.receiver.toString() !== socket.userId
      ) {
        socket.emit("call-error", { error: "Not authorized to end this call" });
        return;
      }
      await call.endCall();
      socket.emit("call-ended", {
        callId: call._id,
        status: call.status,
        duration: call.duration,
        callType: call.callType,
      });
      const otherUserId =
        call.caller.toString() === socket.userId
          ? call.receiver.toString()
          : call.caller.toString();
      const otherSocket = activeUsers.get(otherUserId);
      if (otherSocket) {
        io.to(otherSocket.socketId).emit("call-ended", {
          callId: call._id,
          status: call.status,
          duration: call.duration,
          endedBy: socket.user,
          callType: call.callType,
        });
      }
    } catch (error) {
      console.error("Call end error:", error);
      socket.emit("call-error", { error: "Failed to end call" });
    }
  });

  // Handle request to resend offer (when receiver joins from notification)
  socket.on("call-request-offer", async (data) => {
    try {
      const { callId, receiverId } = data || {};
      console.log(
        `📞 Receiver ${receiverId} requesting offer for call:`,
        callId
      );

      if (!callId) {
        socket.emit("call-error", { error: "Call ID is required" });
        return;
      }

      const call = await Call.findById(callId);
      if (!call) {
        socket.emit("call-error", { error: "Call not found" });
        return;
      }

      // Notify the caller to resend the offer
      const callerActive = activeUsers.get(call.caller.toString());
      if (callerActive) {
        console.log(`📞 Notifying caller ${call.caller} to resend offer`);
        io.to(callerActive.socketId).emit("call-resend-offer", {
          callId: call._id,
          receiverId: call.receiver,
          callType: call.callType,
        });
      }
    } catch (error) {
      console.error("Call request offer error:", error);
      socket.emit("call-error", { error: "Failed to request offer" });
    }
  });

  socket.on("call-offer", async (data) => {
    try {
      const { callId, offer } = data || {};
      if (!callId || !offer) {
        socket.emit("call-error", { error: "Call ID and offer are required" });
        return;
      }
      const call = await Call.findById(callId);
      if (!call) {
        socket.emit("call-error", { error: "Call not found" });
        return;
      }
      if (call.caller.toString() !== socket.userId) {
        socket.emit("call-error", { error: "Not authorized to send offer" });
        return;
      }
      call.offer = JSON.stringify(offer);
      await call.save();
      const receiverActive = activeUsers.get(call.receiver.toString());
      if (receiverActive) {
        io.to(receiverActive.socketId).emit("call-offer", {
          callId: call._id,
          offer,
          from: socket.user,
          callType: call.callType,
        });
      }
    } catch (error) {
      console.error("Call offer error:", error);
      socket.emit("call-error", { error: "Failed to send call offer" });
    }
  });

  socket.on("call-answer-webrtc", async (data) => {
    try {
      const { callId, answer } = data || {};
      if (!callId || !answer) {
        socket.emit("call-error", { error: "Call ID and answer are required" });
        return;
      }
      const call = await Call.findById(callId);
      if (!call) {
        socket.emit("call-error", { error: "Call not found" });
        return;
      }
      if (call.receiver.toString() !== socket.userId) {
        socket.emit("call-error", { error: "Not authorized to send answer" });
        return;
      }
      call.answer = JSON.stringify(answer);
      await call.save();
      const callerActive = activeUsers.get(call.caller.toString());
      if (callerActive) {
        io.to(callerActive.socketId).emit("call-answer-webrtc", {
          callId: call._id,
          answer,
          from: socket.user,
          callType: call.callType,
        });
      }
    } catch (error) {
      console.error("WebRTC answer error:", error);
      socket.emit("call-error", { error: "Failed to send answer" });
    }
  });

  socket.on("ice-candidate", async (data) => {
    try {
      const { callId, candidate, sdpMLineIndex, sdpMid } = data || {};
      if (!callId || !candidate) {
        socket.emit("call-error", {
          error: "Call ID and candidate are required",
        });
        return;
      }
      const call = await Call.findById(callId);
      if (!call) {
        socket.emit("call-error", { error: "Call not found" });
        return;
      }
      // Save candidate (optional)
      try {
        call.iceCandidates.push({ candidate, sdpMLineIndex, sdpMid });
        await call.save();
      } catch (_) {}
      const otherUserId =
        call.caller.toString() === socket.userId
          ? call.receiver.toString()
          : call.caller.toString();
      const otherSocket = activeUsers.get(otherUserId);
      if (otherSocket) {
        io.to(otherSocket.socketId).emit("ice-candidate", {
          callId: call._id,
          candidate,
          sdpMLineIndex,
          sdpMid,
          from: socket.user,
          callType: call.callType,
        });
      }
    } catch (error) {
      console.error("ICE candidate error:", error);
      socket.emit("call-error", { error: "Failed to send ICE candidate" });
    }
  });

  // ========================
  // Group Call signaling handlers
  // ========================
  socket.on("group-call-initiate", async (data) => {
    try {
      const { groupId, callType = "voice", callId } = data || {};
      console.log(`📞 Group call initiate request:`, {
        groupId,
        callType,
        callId,
        initiator: socket.userId,
        initiatorName: socket.user?.name,
      });

      if (!groupId) {
        socket.emit("group-call-error", { error: "Group ID is required" });
        return;
      }

      // Get the GroupCall to fetch roomName
      const GroupCall = require("./models/GroupCall");
      const groupCall = await GroupCall.findById(callId);
      const roomName = groupCall?.roomName;

      console.log("🎥 ========================================");
      console.log("🎥 NOTIFYING GROUP MEMBERS");
      console.log("🎥 Room Name:", roomName);
      console.log("🎥 Call ID:", callId);
      console.log("🎥 ========================================");

      // Notify initiator (confirmation)
      socket.emit("group-call-initiated", {
        callId,
        callType,
        groupId,
        roomName, // ✅ Include roomName!
        initiator: socket.user,
      });

      // Notify all group members about the new call
      const Group = require("./models/Group");
      const group = await Group.findById(groupId).populate(
        "members.user",
        "name avatar email"
      );

      if (group) {
        const initiatorName = socket.user?.name || "Someone";
        const callTypeText = callType === "video" ? "Video" : "Voice";

        group.members.forEach((member) => {
          const memberId = member.user._id.toString();
          const memberSocketData = activeUsers.get(memberId);

          if (memberId !== socket.userId) {
            if (memberSocketData) {
              // Member is online - send socket event
              console.log(
                `📤 Notifying ${member.user.name} - Room: ${roomName}`
              );
              io.to(memberSocketData.socketId).emit("incoming-group-call", {
                callId,
                callType,
                groupId,
                groupName: group.name,
                roomName, // ✅ CRITICAL: Include roomName for incoming calls!
                initiator: socket.user,
                group: {
                  name: group.name,
                  avatar: group.avatar,
                  _id: group._id,
                },
              });
            } else {
              // Member is offline - send push notification
              console.log(
                `📤 Sending push notification to offline member: ${member.user.name}`
              );

              sendFCMToUser(
                memberId,
                `📞 Incoming Group ${callTypeText} Call`,
                `${initiatorName} is calling in ${group.name}`,
                {
                  type: "incoming-group-call",
                  callType: callType,
                  callId: callId,
                  groupId: groupId,
                  groupName: group.name,
                  roomName: roomName,
                  initiatorId: socket.userId,
                  initiatorName: initiatorName,
                  // Add fields that frontend expects
                  senderId: socket.userId,
                  senderName: initiatorName,
                  senderImage: socket.user?.profileImage || "/icon.png",
                  icon: socket.user?.profileImage || "/icon.png",
                }
              )
                .then((result) => {
                  if (!result.success) {
                    return sendPushToUser(
                      memberId,
                      `📞 Incoming Group ${callTypeText} Call`,
                      `${initiatorName} is calling in ${group.name}`,
                      socket.user?.profileImage || "/icon.png",
                      {
                        type: "incoming-group-call",
                        callType: callType,
                        callId: callId,
                        groupId: groupId,
                        groupName: group.name,
                        roomName: roomName,
                        initiatorId: socket.userId,
                        initiatorName: initiatorName,
                        // Add fields that frontend expects
                        senderId: socket.userId,
                        senderName: initiatorName,
                        senderImage: socket.user?.profileImage || "/icon.png",
                      }
                    );
                  }
                })
                .catch((err) => {
                  console.log(
                    `Push notification failed for ${member.user.name}:`,
                    err.message
                  );
                });
            }
          }
        });
      }
    } catch (error) {
      console.error("Group call initiate error:", error);
      socket.emit("group-call-error", {
        error: "Failed to initiate group call",
      });
    }
  });

  socket.on("group-call-join", async (data) => {
    try {
      const { callId, groupId } = data || {};
      console.log(`📞 Group call join request:`, {
        callId,
        groupId,
        userId: socket.userId,
        userName: socket.user?.name,
      });

      if (!callId || !groupId) {
        socket.emit("group-call-error", {
          error: "Call ID and Group ID are required",
        });
        return;
      }

      // Notify all other participants in the call
      const GroupCall = require("./models/GroupCall");
      const groupCall = await GroupCall.findById(callId).populate(
        "participants.user",
        "name avatar email"
      );

      if (groupCall) {
        groupCall.participants.forEach((participant) => {
          if (
            participant.user._id.toString() !== socket.userId &&
            participant.isActive
          ) {
            const participantSocketData = activeUsers.get(
              participant.user._id.toString()
            );
            if (participantSocketData) {
              const newParticipantData = {
                id: socket.user._id.toString(),
                name: socket.user.name,
                email: socket.user.email,
                role: socket.user.role,
                isActive: true,
              };
              console.log(
                `📤 Notifying participant ${participant.user.name} about new joiner:`,
                {
                  callId,
                  groupId,
                  newParticipant: newParticipantData,
                }
              );
              io.to(participantSocketData.socketId).emit(
                "group-call-participant-joined",
                {
                  callId,
                  groupId,
                  newParticipant: newParticipantData,
                }
              );
            }
          }
        });
      }

      // Confirm join to the user
      socket.emit("group-call-joined", {
        callId,
        groupId,
        participant: socket.user,
      });
    } catch (error) {
      console.error("Group call join error:", error);
      socket.emit("group-call-error", { error: "Failed to join group call" });
    }
  });

  socket.on("group-call-leave", async (data) => {
    try {
      const { callId, groupId } = data || {};
      console.log(`📞 Group call leave request:`, {
        callId,
        groupId,
        userId: socket.userId,
        userName: socket.user?.name,
      });

      if (!callId || !groupId) {
        socket.emit("group-call-error", {
          error: "Call ID and Group ID are required",
        });
        return;
      }

      // Notify all other participants in the call
      const GroupCall = require("./models/GroupCall");
      const groupCall = await GroupCall.findById(callId).populate(
        "participants.user",
        "name avatar email"
      );

      if (groupCall) {
        // Check if the leaving user is the initiator
        const isInitiator = groupCall.initiator.toString() === socket.userId;

        if (isInitiator) {
          // If initiator leaves, end the call for everyone
          console.log(
            `📞 Initiator ${socket.user?.name} left, ending call for all participants`
          );

          // End the call in database
          await groupCall.endCall();

          // Notify all participants that call has ended
          groupCall.participants.forEach((participant) => {
            if (participant.isActive) {
              const participantSocketData = activeUsers.get(
                participant.user._id.toString()
              );
              if (participantSocketData) {
                console.log(
                  `📤 Notifying participant ${participant.user.name} about call end`
                );
                io.to(participantSocketData.socketId).emit("group-call-ended", {
                  callId,
                  groupId,
                  reason: "Initiator left the call",
                  endedBy: socket.user,
                });
              }
            }
          });
        } else {
          // Regular participant leaving, just notify others
          groupCall.participants.forEach((participant) => {
            if (
              participant.user._id.toString() !== socket.userId &&
              participant.isActive
            ) {
              const participantSocketData = activeUsers.get(
                participant.user._id.toString()
              );
              if (participantSocketData) {
                console.log(
                  `📤 Notifying participant ${participant.user.name} about leaver`
                );
                io.to(participantSocketData.socketId).emit(
                  "group-call-participant-left",
                  {
                    callId,
                    groupId,
                    leftParticipant: socket.user,
                  }
                );
              }
            }
          });
        }
      }

      // Confirm leave to the user
      socket.emit("group-call-left", {
        callId,
        groupId,
        participant: socket.user,
      });
    } catch (error) {
      console.error("Group call leave error:", error);
      socket.emit("group-call-error", { error: "Failed to leave group call" });
    }
  });

  socket.on("group-call-end", async (data) => {
    try {
      const { callId, groupId } = data || {};
      console.log(`📞 Group call end request:`, {
        callId,
        groupId,
        userId: socket.userId,
        userName: socket.user?.name,
      });

      if (!callId || !groupId) {
        socket.emit("group-call-error", {
          error: "Call ID and Group ID are required",
        });
        return;
      }

      // Notify all participants in the call
      const GroupCall = require("./models/GroupCall");
      const groupCall = await GroupCall.findById(callId).populate(
        "participants.user",
        "name avatar email"
      );

      if (groupCall) {
        groupCall.participants.forEach((participant) => {
          if (participant.isActive) {
            const participantSocketData = activeUsers.get(
              participant.user._id.toString()
            );
            if (participantSocketData) {
              console.log(
                `📤 Notifying participant ${participant.user.name} about call end`
              );
              io.to(participantSocketData.socketId).emit("group-call-ended", {
                callId,
                groupId,
                endedBy: socket.user,
              });
            }
          }
        });
      }
    } catch (error) {
      console.error("Group call end error:", error);
      socket.emit("group-call-error", { error: "Failed to end group call" });
    }
  });

  // Group call WebRTC signaling
  socket.on("group-call-offer", async (data) => {
    try {
      const { callId, groupId, targetUserId, offer } = data || {};
      console.log(`📞 Group call offer:`, {
        callId,
        groupId,
        from: socket.userId,
        to: targetUserId,
      });

      if (!callId || !groupId || !targetUserId || !offer) {
        socket.emit("group-call-error", {
          error: "Call ID, Group ID, target user ID, and offer are required",
        });
        return;
      }

      const targetSocketData = activeUsers.get(targetUserId);
      if (targetSocketData) {
        io.to(targetSocketData.socketId).emit("group-call-offer", {
          callId,
          groupId,
          from: socket.user,
          offer,
        });
      }
    } catch (error) {
      console.error("Group call offer error:", error);
      socket.emit("group-call-error", {
        error: "Failed to send group call offer",
      });
    }
  });

  socket.on("group-call-answer", async (data) => {
    try {
      const { callId, groupId, targetUserId, answer } = data || {};
      console.log(`📞 Group call answer:`, {
        callId,
        groupId,
        from: socket.userId,
        to: targetUserId,
      });

      if (!callId || !groupId || !targetUserId || !answer) {
        socket.emit("group-call-error", {
          error: "Call ID, Group ID, target user ID, and answer are required",
        });
        return;
      }

      const targetSocketData = activeUsers.get(targetUserId);
      if (targetSocketData) {
        io.to(targetSocketData.socketId).emit("group-call-answer", {
          callId,
          groupId,
          from: socket.user,
          answer,
        });
      }
    } catch (error) {
      console.error("Group call answer error:", error);
      socket.emit("group-call-error", {
        error: "Failed to send group call answer",
      });
    }
  });

  socket.on("group-call-ice-candidate", async (data) => {
    try {
      console.log(`📞 Group call ICE candidate received:`, data);
      const {
        callId,
        groupId,
        targetUserId,
        candidate,
        sdpMLineIndex,
        sdpMid,
      } = data || {};
      console.log(`📞 Group call ICE candidate parsed:`, {
        callId,
        groupId,
        from: socket.userId,
        to: targetUserId,
        hasCandidate: !!candidate,
      });

      if (!callId || !groupId || !targetUserId || !candidate) {
        socket.emit("group-call-error", {
          error:
            "Call ID, Group ID, target user ID, and candidate are required",
        });
        return;
      }

      const targetSocketData = activeUsers.get(targetUserId);
      if (targetSocketData) {
        io.to(targetSocketData.socketId).emit("group-call-ice-candidate", {
          callId,
          groupId,
          from: socket.user,
          candidate,
          sdpMLineIndex,
          sdpMid,
        });
      }
    } catch (error) {
      console.error("Group call ICE candidate error:", error);
      socket.emit("group-call-error", {
        error: "Failed to send group call ICE candidate",
      });
    }
  });

  // Handle force disconnect (immediate disconnect when closing tab)
  socket.on("force-disconnect", async (data) => {
    const lastSeenTime = new Date();

    console.log(`🚪 Force disconnect: ${socket.user?.name || socket.userId}`);

    // Update user lastSeen in database
    try {
      await User.findByIdAndUpdate(socket.userId, {
        lastSeen: lastSeenTime,
      });
    } catch (err) {
      console.error("Error updating lastSeen:", err);
    }

    // Immediately remove from active users
    activeUsers.delete(socket.userId);

    // Broadcast offline status immediately
    socket.broadcast.emit("user-offline", {
      userId: socket.userId,
      user: {
        _id: socket.userId,
        name: socket.user?.name,
        email: socket.user?.email,
        profileImage: socket.user?.profileImage,
      },
      lastSeen: lastSeenTime,
    });

    // Disconnect socket
    socket.disconnect(true);
  });

  // Handle user away (tab hidden but not closed)
  socket.on("user-away", async (data) => {
    console.log(`📴 User away: ${socket.user?.name || socket.userId}`);

    // Remove from active users so push notifications will be sent
    activeUsers.delete(socket.userId);

    const lastSeenTime = new Date();
    await User.findByIdAndUpdate(socket.userId, {
      lastSeen: lastSeenTime,
    }).catch((err) => console.error("Error updating lastSeen:", err));

    // Broadcast offline status
    socket.broadcast.emit("user-offline", {
      userId: socket.userId,
      user: {
        _id: socket.userId,
        name: socket.user?.name,
        email: socket.user?.email,
        profileImage: socket.user?.profileImage,
      },
      lastSeen: lastSeenTime,
    });
  });

  // Handle user active (tab visible again)
  socket.on("user-active", async (data) => {
    console.log(`✅ User active: ${socket.user?.name || socket.userId}`);

    // Add back to active users
    activeUsers.set(socket.userId, {
      socketId: socket.id,
      user: socket.user,
      lastSeen: new Date(),
    });

    // Broadcast online status
    socket.broadcast.emit("user-online", {
      userId: socket.userId,
      user: {
        _id: socket.userId,
        name: socket.user?.name,
        email: socket.user?.email,
        profileImage: socket.user?.profileImage,
      },
      lastSeen: new Date(),
    });
  });

  // Handle disconnect
  socket.on("disconnect", async () => {
    const lastSeenTime = new Date();

    // Update user lastSeen in database
    try {
      await User.findByIdAndUpdate(socket.userId, {
        lastSeen: lastSeenTime,
      });
    } catch (err) {
      console.error("Error updating lastSeen on disconnect:", err);
    }

    // Immediately remove from active users
    activeUsers.delete(socket.userId);

    // Broadcast to all connected users that this user is now offline
    socket.broadcast.emit("user-offline", {
      userId: socket.userId,
      user: {
        _id: socket.userId,
        name: socket.user?.name,
        email: socket.user?.email,
        profileImage: socket.user?.profileImage,
      },
      lastSeen: lastSeenTime,
    });

    console.log(
      `❌ User disconnected: ${socket.user?.name || socket.userId} - ${
        socket.id
      }`
    );
  });
});

// Database connection
mongoose
  .connect(process.env.MONGODB_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  })
  .then(async () => {
    console.log("Connected to MongoDB");

    // Sync with NTP server first (get global time, not system time)

    try {
      await syncWithNTPServer();
    } catch (error) {}

    // Initialize auto-disable scheduler after DB connection and NTP sync
    initializeAutoDisableScheduler();

    // Re-sync with NTP server every 1 hour to maintain accuracy
    setInterval(async () => {
      console.log("🔄 Re-syncing with NTP server...");
      try {
        await syncWithNTPServer();
      } catch (error) {
        console.warn("⚠️  NTP re-sync failed, continuing with last offset");
      }
    }, 60 * 60 * 1000); // 1 hour
  })
  .catch((error) => {
    console.error("MongoDB connection error:", error);
  });

// Global time offset from NTP server (in milliseconds)
let globalTimeOffset = 0;
let lastNTPSync = null;

// Helper function to get global time from NTP server
async function syncWithNTPServer() {
  return new Promise((resolve, reject) => {
    // Priority NTP servers list (AWS NTP server first for AWS deployments)
    const ntpServers = [
      "169.254.169.123", // AWS NTP server (works only on AWS EC2)
      "time.google.com", // Google NTP server
      "pool.ntp.org", // Public NTP pool
    ];

    let currentServerIndex = 0;

    function tryNextServer() {
      if (currentServerIndex >= ntpServers.length) {
        reject(new Error("All NTP servers failed"));
        return;
      }

      const server = ntpServers[currentServerIndex];
      const serverName =
        server === "169.254.169.123"
          ? "AWS NTP"
          : server === "time.google.com"
          ? "Google NTP"
          : "Pool NTP";

      ntpClient.getNetworkTime(server, 123, (err, date) => {
        if (err) {
          currentServerIndex++;
          tryNextServer();
        } else {
          const systemTime = new Date();
          globalTimeOffset = date.getTime() - systemTime.getTime();
          lastNTPSync = date;

          resolve(date);
        }
      });
    }

    tryNextServer();
  });
}

// Helper function to get current IST time (using global NTP time, NOT system time)
function getCurrentISTTime() {
  // Get global time = system time + NTP offset
  const globalTime = new Date(Date.now() + globalTimeOffset);
  // Convert global time to IST
  return moment(globalTime).tz("Asia/Kolkata");
}

// Initialize the scheduler
async function initializeAutoDisableScheduler() {
  try {
    const istNow = getCurrentISTTime();

    // Run every minute to check if it's time to enable/disable users (using IST)
    cron.schedule("* * * * *", async () => {
      const istNow = getCurrentISTTime();
      const currentTime = istNow.format("HH:mm");
      const currentDay = istNow.format("dddd"); // Monday, Tuesday, etc.
      const currentDate = istNow.format("YYYY-MM-DD");

      // Check user-specific schedules (enable and disable)
      const activeSchedules = await ScheduledDisable.find({
        isActive: true,
        days: currentDay,
        $or: [{ enableTime: currentTime }, { disableTime: currentTime }],
      }).populate("users", "name email");

      for (const schedule of activeSchedules) {
        let actionTriggered = false;

        // Check ENABLE time
        if (schedule.enableTime && schedule.enableTime === currentTime) {
          const lastTriggeredEnableDate = schedule.lastTriggeredEnable
            ? moment(schedule.lastTriggeredEnable)
                .tz("Asia/Kolkata")
                .format("YYYY-MM-DD")
            : null;

          // Skip if already triggered enable today
          if (lastTriggeredEnableDate !== currentDate) {
            console.log(
              `📅 Scheduled ENABLE triggered: "${schedule.name}" at ${currentTime}`
            );
            actionTriggered = true;

            // ENABLE users
            if (schedule.applyToAllUsers) {
              // Enable all regular users
              const result = await User.updateMany(
                { role: "user", isActive: false },
                {
                  $set: {
                    isActive: true,
                    disabledAt: null,
                    disableReason: null,
                  },
                }
              );

              console.log(
                `✅ Enabled ${result.modifiedCount} users (All Users) via schedule: ${schedule.name}`
              );
            } else if (schedule.users && schedule.users.length > 0) {
              // Enable specific users
              const userIds = schedule.users.map((u) => u._id);
              const result = await User.updateMany(
                { _id: { $in: userIds }, isActive: false },
                {
                  $set: {
                    isActive: true,
                    disabledAt: null,
                    disableReason: null,
                  },
                }
              );

              console.log(
                `✅ Enabled ${result.modifiedCount} specific users via schedule: ${schedule.name}`
              );
            }

            // Update last triggered enable time
            schedule.lastTriggeredEnable = istNow.toDate();
            await schedule.save();
          }
        }

        // Check DISABLE time
        if (schedule.disableTime && schedule.disableTime === currentTime) {
          const lastTriggeredDisableDate = schedule.lastTriggeredDisable
            ? moment(schedule.lastTriggeredDisable)
                .tz("Asia/Kolkata")
                .format("YYYY-MM-DD")
            : null;

          // Skip if already triggered disable today
          if (lastTriggeredDisableDate !== currentDate) {
            console.log(
              `📅 Scheduled DISABLE triggered: "${schedule.name}" at ${currentTime}`
            );

            // DISABLE users
            if (schedule.applyToAllUsers) {
              // Disable all regular users
              const result = await User.updateMany(
                { role: "user", isActive: true },
                {
                  $set: {
                    isActive: false,
                    disabledAt: istNow.toDate(),
                    disableReason: "auto",
                  },
                }
              );

              console.log(
                `✅ Disabled ${result.modifiedCount} users (All Users) via schedule: ${schedule.name}`
              );

              // Force logout
              const disabledUsers = await User.find({
                role: "user",
                isActive: false,
                disableReason: "auto",
              });

              disabledUsers.forEach((user) => {
                try {
                  io.to(user._id.toString()).emit("force-logout", {
                    message: `Your account has been automatically disabled (Schedule: ${schedule.name})`,
                    reason: "scheduled_disable",
                    scheduleName: schedule.name,
                    disabledAt: istNow.toDate(),
                  });
                } catch (err) {
                  console.log("Socket emit failed for user:", user._id);
                }
              });
            } else if (schedule.users && schedule.users.length > 0) {
              // Disable specific users
              const userIds = schedule.users.map((u) => u._id);
              const result = await User.updateMany(
                { _id: { $in: userIds }, isActive: true },
                {
                  $set: {
                    isActive: false,
                    disabledAt: istNow.toDate(),
                    disableReason: "auto",
                  },
                }
              );

              console.log(
                `✅ Disabled ${result.modifiedCount} specific users via schedule: ${schedule.name}`
              );

              // Force logout specific users
              schedule.users.forEach((user) => {
                try {
                  io.to(user._id.toString()).emit("force-logout", {
                    message: `Your account has been automatically disabled (Schedule: ${schedule.name})`,
                    reason: "scheduled_disable",
                    scheduleName: schedule.name,
                    disabledAt: istNow.toDate(),
                  });
                } catch (err) {
                  console.log("Socket emit failed for user:", user._id);
                }
              });
            }

            // Update last triggered disable time
            schedule.lastTriggeredDisable = istNow.toDate();
            await schedule.save();
          }
        }
      }
    });
  } catch (error) {
    console.error("❌ Error initializing auto-disable scheduler:", error);
  }
}

// Catch-all handler: send back React's index.html file for any non-API routes
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "dist", "index.html"));
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ message: "Something went wrong!" });
});

const PORT = process.env.PORT;

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

module.exports = { app, io };
