package gui;

import java.awt.*;
import java.awt.image.BaseMultiResolutionImage;
import java.awt.image.BufferedImage;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import dna.Dna;
import logger.LogEvent;
import logger.Logger;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.ImageTranscoder;

import javax.swing.*;

public class SvgIcon {
    private BaseMultiResolutionImage b;

    /**
     * Create an instance of an SvgIcon by supplying the path to an SVG file, including file name and the desired
     * baseline size.
     *
     * @param svgPath The path to an SVG file, for example {@code "/icons/tabler_trash.svg"}.
     * @param baselineSize The baseline size of the diagram. The image is saved in multiple resolutions, and the
     *                     baseline size is the size of the image with a scaling factor of 1.0. Images are required to
     *                     be square, i.e., the size is both the height and the width in pixels. For example, if a
     *                     baseline size of {@code 14} is supplied, the first image has a width and height of 14px, and
     *                     the remaining images increase in size with scaling factors 1.25, 1.5, 1.75, ..., 3.0 up to a
     *                     size of 3.0 x 14 = 42px.
     */
    public SvgIcon(String svgPath, int baselineSize) {
        this.b = this.createMultiImage(svgPath, baselineSize, null);
    }

    /**
     * Create an instance of an SvgIcon by supplying the path to an SVG file, including file name, the desired baseline
     * size, and an optional background color.
     *
     * @param svgPath The path to an SVG file, for example {@code "/icons/tabler_trash.svg"}.
     * @param baselineSize The baseline size of the diagram. The image is saved in multiple resolutions, and the
     *                     baseline size is the size of the image with a scaling factor of 1.0. Images are required to
     *                     be square, i.e., the size is both the height and the width in pixels. For example, if a
     *                     baseline size of {@code 14} is supplied, the first image has a width and height of 14px, and
     *                     the remaining images increase in size with scaling factors 1.25, 1.5, 1.75, ..., 3.0 up to a
     *                     size of 3.0 x 14 = 42px.
     * @param backgroundColor If {@code null}, do not change the background color of the SVG diagram. If a color is
     *                        provided, use this color as the new background color for the diagram, for example
     *                        {@code java.awt.Color.ORANGE}.
     */
    public SvgIcon(String svgPath, int baselineSize, Color backgroundColor) {
        this.b = this.createMultiImage(svgPath, baselineSize, backgroundColor);
    }

    /**
     * Get an image icon version of the multi-resolution image.
     *
     * @return An {@link ImageIcon} object with multiple resolutions.
     */
    public ImageIcon getImageIcon() {
        return new ImageIcon(this.b);
    }

    /**
     * Get the multi-resolution image.
     *
     * @return A {@link BaseMultiResolutionImage} object with multiple resolutions.
     */
    public BaseMultiResolutionImage getImage() {
        return b;
    }

    private BaseMultiResolutionImage createMultiImage(String svgPath, int baselineSize, Color backgroundColor) {
        double[] zoomFactors = {1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0};
        List<BufferedImage> images = new ArrayList<>();
        for (double zoomFactor : zoomFactors) {
            int size = (int) (baselineSize * zoomFactor);
            BufferedImage image = transcodeSvgToImage(svgPath, size, size, backgroundColor);
            images.add(image);
        }
        return new BaseMultiResolutionImage(images.stream().toArray(BufferedImage[]::new));
    }

    private BufferedImage transcodeSvgToImage(String svgPath, int width, int height, Color backgroundColor) {
        String inputString = null;
        try {
            inputString = getClass().getResource(svgPath).toURI().toString();
        } catch (URISyntaxException e) {
            LogEvent l = new LogEvent(Logger.ERROR,
                    "Failed to convert SVG resource to URI.",
                    "Tried to retrieve the SVG file " + svgPath + " from the JAR container, but failed to convert it into a URI string.",
                    e);
            Dna.logger.log(l);
        }
        BufferedImageTranscoder transcoder = new BufferedImageTranscoder();
        transcoder.addTranscodingHint(ImageTranscoder.KEY_WIDTH, (float) width);
        transcoder.addTranscodingHint(ImageTranscoder.KEY_HEIGHT, (float) height);
        if (backgroundColor != null) {
            transcoder.addTranscodingHint(ImageTranscoder.KEY_BACKGROUND_COLOR, backgroundColor);
        }
        TranscoderInput input = new TranscoderInput(inputString);
        try {
            transcoder.transcode(input, null);
        } catch (TranscoderException e) {
            LogEvent l = new LogEvent(Logger.ERROR,
                    "Failed to transcode SVG image.",
                    "Tried to transcode SVG file " + svgPath + " into a rasterized image for use as an icon.",
                    e);
            Dna.logger.log(l);
        }
        return transcoder.getImage();
    }

    private static class BufferedImageTranscoder extends ImageTranscoder {
        private BufferedImage image = null;

        public BufferedImage getImage() {
            return image;
        }

        @Override
        public BufferedImage createImage(int w, int h) {
            BufferedImage bi = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
            return bi;
        }

        @Override
        public void writeImage(BufferedImage img, TranscoderOutput output) {
            image = img;
        }
    }
}