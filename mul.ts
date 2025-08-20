import multer from "multer";
import path from "path";

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "uploads/events/"); // save files here
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname)); // unique name
  }
});

export const upload = multer({ storage });



import express from "express";
import Event from "../models/Event";
import Image from "../models/Image";
import { upload } from "../middlewares/upload";

const router = express.Router();

// ✅ Create a new Event
router.post("/", async (req, res) => {
  try {
    const { title, description, date, location, status, category, totalSeats, availableSeats, organiser } = req.body;

    const event = new Event({
      title,
      description,
      date,
      location,
      status,
      category,
      totalSeats,
      availableSeats,
      organiser
    });

    await event.save();
    res.status(201).json({ message: "Event created successfully", event });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ Add images to an Event
router.post("/:eventId/images", upload.array("images", 5), async (req, res) => {
  try {
    const { eventId } = req.params;
    const event = await Event.findById(eventId);

    if (!event) return res.status(404).json({ message: "Event not found" });

    const files = req.files as Express.Multer.File[];
    const imageDocs = await Promise.all(
      files.map(file =>
        Image.create({
          url: `/uploads/events/${file.filename}`,
          event: event._id
        })
      )
    );

    res.status(201).json({ message: "Images uploaded successfully", images: imageDocs });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ Get event with its images
router.get("/:id", async (req, res) => {
  try {
    const event = await Event.findById(req.params.id).populate("organiser", "name email");
    if (!event) return res.status(404).json({ message: "Event not found" });

    const images = await Image.find({ event: event._id });

    res.json({ ...event.toObject(), images });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
