import com.google.common.base.Splitter;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CoinbaseCsvReader {

    private static Splitter splitter = Splitter.on(',').trimResults();

    public static void main(String[] args) throws IOException, ParseException {
        String csvFile = "E:\\Downloads\\fills.csv";
        List<String> lines = readFile(csvFile);

        int counter = 0;
        List<Order> orders = new ArrayList<>();

        for (String csv : lines) {
            List<String> parts = splitter.splitToList(csv);

            if (counter++ == 0) {
                if (!validateHeader(parts)) {
                    throw new IOException("Unexpected CSV header");
                }
                continue;
            }

            Order row = Order.of(parts);
            orders.add(row);
        }

        orders.sort((t1, t2) ->
                ComparisonChain.start()
                        .compare(t1.product, t2.product)
//                        .compareTrueFirst(t1.buy, t2.buy)
                        .compare(t1.createdAt, t2.createdAt)
                        .result()
        );

        orders.forEach(t -> System.out.println(t.toString()));

        Map<String, List<Order>> grouped = orders.stream().collect(Collectors.groupingBy(o -> o.product));

        System.out.println("");
        System.out.println("*********************************************************");
        System.out.println("");

        for (Map.Entry<String, List<Order>> entry : grouped.entrySet()) {
            System.out.println("Security\tOpenDate\tOpen Price\t\t\tQuantity\tFee\tClose Date\t\tClose Price\t\t\tCost Basis");
            List<Transaction> transactions = reconcile(entry.getValue());
            transactions.forEach(t -> System.out.println(t.toString()));
            System.out.println("");
        }

    }

    private static List<String> readFile(String filepath) throws IOException {
        try (Stream<String> stream = Files.lines(Paths.get(filepath))) {
            return stream.collect(Collectors.toList());
        }
    }

    private static boolean validateHeader(List<String> actual) {
        List<String> expected = ImmutableList.of(
                "portfolio",
                "trade id",
                "product",
                "side",
                "created at",
                "size",
                "size unit",
                "price",
                "fee",
                "total",
                "price/fee/total unit"
        );

        return expected.equals(actual);
    }

    private static List<Transaction> reconcile(List<Order> orders) {
        List<Transaction> transactions = new LinkedList<>();

        for (Order order : orders) {
            if (order.buy) {
                Transaction t = new Transaction(order);
                transactions.add(t);
            } else {
                Order remaining;
                do {
                    remaining = match(transactions, order);
                } while (remaining != null);
            }
        }

        return transactions;
    }

    /**
     * @return any remainder of sell
     */
    private static Order match(List<Transaction> transactions, Order sell) {
        // LIFO
        ListIterator<Transaction> iterator = transactions.listIterator();
        while (iterator.hasNext()) {
            Transaction transaction = iterator.next();
            if (transaction.buy != null && transaction.sell != null) {
                continue;
            }

            assert transaction.buy != null;
            final int result = Double.compare(transaction.buy.size, sell.size);

            if (result == 0) {
                transaction.sell = sell;
                return null;

            } else if (result > 0) {
                iterator.remove();
                Order buy = transaction.buy;

                Order remainingBuy = buy.clone();
                buy.size = sell.size;
                buy.total = buy.size * buy.price + buy.fee;
                remainingBuy.size -= sell.size;
                remainingBuy.fee = 0.0;
                remainingBuy.total = remainingBuy.size + remainingBuy.price;

                iterator.add(new Transaction(buy, sell));
                iterator.add(new Transaction(remainingBuy, null));
                return null;

            } else { // transaction.buy.size < sell.size
                Order remainingSell = sell.clone();
                remainingSell.size = remainingSell.size - transaction.buy.size;
                remainingSell.fee = 0.0;
                remainingSell.total = remainingSell.size + remainingSell.price;

                sell.size = transaction.buy.size;
                sell.total = sell.size * sell.price + sell.fee;
                transaction.sell = sell;

                return remainingSell;
            }
        }

        return null;
    }

    /**
     * Example:
     * default, 7204216, ADA-USD, BUY, 2021-05-07T16:17:11.940Z, 589.19000000, ADA, 1.7039, 5.019604205, -1008.940445205, USD
     */
    private static class Order implements Cloneable {
        public String tradeId;
        public String product;
        public boolean buy;
        public Date createdAt;
        public double size;
        public double price;
        public double fee;
        public double total;

        // 2021-05-07T16:17:11.940Z
        // yyyy-MM-dd'T'HH:mm:ss.SSSZ
        public static Order of(List<String> parts) throws ParseException {
            Order order = new Order();
            order.tradeId = parts.get(1);
            order.product = parts.get(2);
            order.buy = parts.get(3).equalsIgnoreCase("BUY");
            order.createdAt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(parts.get(4));
            order.size = Double.parseDouble(parts.get(5));
            order.price = Double.parseDouble(parts.get(7));
            order.fee = Double.parseDouble(parts.get(8));
            order.total = Double.parseDouble(parts.get(9));
            return order;
        }

        public Order clone() {
            Order cloned = new Order();
            cloned.tradeId = tradeId;
            cloned.product = product;
            cloned.buy = buy;
            cloned.createdAt = createdAt;
            cloned.size = size;
            cloned.price = price;
            cloned.fee = fee;
            cloned.total = total;
            return cloned;
        }

        @Override
        public String toString() {
            return "Transaction{" +
                    "product='" + product + '\'' +
                    ", " + (buy ? "BUY" : "SELL") +
                    ", createdAt=" + createdAt +
                    ", size=" + size +
                    ", price=" + price +
                    ", fee=" + fee +
                    ", total=" + total +
                    '}';
        }
    }

    private static class Transaction {
        public final Order buy;
        public Order sell;

        public Transaction(Order buy) {
            this.buy = buy;
            this.sell = null;
        }

        public Transaction(Order buy, Order sell) {
            this.buy = buy;
            this.sell = sell;
        }

        @Override
        public String toString() {
            SimpleDateFormat format = new SimpleDateFormat("MM-dd-yyyy");
            DecimalFormat dFormat = new DecimalFormat("####.######");

            StringBuilder builder = new StringBuilder();

            builder.append(buy.product).append("\t\t")
                    .append(format.format(buy.createdAt)).append("\t\t")
                    .append(dFormat.format(buy.price)).append("\t\t\t")
                    .append(dFormat.format(buy.size)).append("\t")
                    .append(dFormat.format(buy.fee)).append("\t");

            if (sell != null) {
                builder.append(format.format(sell.createdAt)).append("\t\t")
                        .append(dFormat.format(sell.price)).append("\t\t\t");

                double costBasis = buy.total - sell.total;
                builder.append(dFormat.format(costBasis));
            } else {
                builder.append("\n");
            }

            return builder.toString();
        }
    }
}
