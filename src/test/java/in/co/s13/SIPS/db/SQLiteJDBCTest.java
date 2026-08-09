/*
 * Copyright (C) 2026 Navdeep Singh Sidhu
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package in.co.s13.SIPS.db;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Connection lifecycle.
 *
 * <p>{@code closeConnection()} dereferenced the statement unconditionally, so
 * closing after a connection that never opened threw NullPointerException. The
 * usual caller shape is "try an operation, log any failure, then close", which
 * meant a recoverable database error — a missing directory, say — was turned
 * into a crash by the cleanup that followed it.
 */
class SQLiteJDBCTest {

    @Test
    void closingWithoutEverConnectingDoesNotThrow() {
        assertDoesNotThrow(() -> new SQLiteJDBC().closeConnection());
    }

    @Test
    void closingTwiceDoesNotThrow(@TempDir Path dir) {
        SQLiteJDBC db = new SQLiteJDBC();
        db.update(dir.resolve("t.db").toString(), "CREATE TABLE IF NOT EXISTS T (ID INT);");

        assertDoesNotThrow(db::closeConnection);
        assertDoesNotThrow(db::closeConnection);
    }

    @Test
    void closingAfterAFailedConnectionDoesNotThrow(@TempDir Path dir) {
        // The exact sequence from the sample run: the parent directory does not
        // exist, the update logs its failure, and then the caller closes.
        SQLiteJDBC db = new SQLiteJDBC();
        db.update(dir.resolve("missing-dir").resolve("t.db").toString(),
                "CREATE TABLE IF NOT EXISTS T (ID INT);");

        assertDoesNotThrow(db::closeConnection);
    }

    @Test
    void aConnectionObjectIsReusableAfterAFailure(@TempDir Path dir) {
        SQLiteJDBC db = new SQLiteJDBC();
        db.update(dir.resolve("nope").resolve("t.db").toString(), "CREATE TABLE T (ID INT);");
        db.closeConnection();

        // Same object, now pointed at a usable path.
        assertDoesNotThrow(() -> {
            db.update(dir.resolve("ok.db").toString(), "CREATE TABLE IF NOT EXISTS T (ID INT);");
            db.closeConnection();
        });
    }
}
